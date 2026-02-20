import csv
import io
import os
import re
import sqlite3
import json
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from flask import Flask, render_template, request, redirect, url_for, flash

APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "data", "app.db")

STAGES = [
    ("PRE_FORECLOSURE", "Pre-Foreclosure"),
    ("FORECLOSURE_SALE", "Foreclosure / Sale"),
    ("REO", "REO / Bank-Owned"),
    ("TAX_DELINQUENCY", "Tax Delinquency"),
    ("OTHER", "Other"),
]

def db():
    os.makedirs(os.path.join(APP_DIR, "data"), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stage TEXT NOT NULL,
        apn TEXT,
        address TEXT NOT NULL,
        city TEXT NOT NULL,
        state TEXT NOT NULL,
        zip TEXT,
        record_date TEXT,
        doc_type TEXT,
        source_url TEXT
    );

    CREATE TABLE IF NOT EXISTS snapshots (
        run_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        key TEXT NOT NULL,
        hash TEXT NOT NULL,
        PRIMARY KEY (run_id, item_id)
    );
    """)
    conn.commit()

    # --- lightweight migration for new columns ---
    cols = {r["name"] for r in cur.execute("PRAGMA table_info(items)").fetchall()}
    if "resolved_situs" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN resolved_situs TEXT")
    if "assessor_url" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN assessor_url TEXT")
    conn.commit()
    conn.close()

def norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def make_key(row) -> str:
    apn = norm(row["apn"] or "")
    addr = norm(row["address"])
    city = norm(row["city"])
    stage = norm(row["stage"])
    if apn:
        return f"{stage}|apn:{apn}"
    return f"{stage}|addr:{addr}|{city}"

def make_hash(row) -> str:
    parts = [
        row["stage"], row["apn"] or "", row["address"], row["city"], row["state"],
        row["zip"] or "", row["record_date"] or "", row["doc_type"] or "",
        row["source_url"] or "", row["resolved_situs"] or "", row["assessor_url"] or ""
    ]
    return "|".join([norm(p) for p in parts])

def seed_tax_examples():
    conn = db()
    cur = conn.cursor()
    existing = cur.execute("SELECT COUNT(*) c FROM items WHERE stage='TAX_DELINQUENCY'").fetchone()["c"]
    if existing:
        conn.close()
        return
    path = os.path.join(APP_DIR, "seed_tax_examples.json")
    data = json.load(open(path, "r"))
    for r in data:
        cur.execute(
            """INSERT INTO items (stage, apn, address, city, state, zip, record_date, doc_type, source_url)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (r["stage"], r.get("apn"), r["address"], r["city"], r["state"], r.get("zip"),
             r.get("record_date") or None, r.get("doc_type"), r.get("source_url"))
        )
    conn.commit()
    conn.close()

def create_run_and_snapshot():
    conn = db()
    cur = conn.cursor()
    created_at = datetime.now(timezone.utc).isoformat()
    cur.execute("INSERT INTO runs (created_at) VALUES (?)", (created_at,))
    run_id = cur.lastrowid

    items = cur.execute("SELECT * FROM items").fetchall()
    for it in items:
        k = make_key(it)
        h = make_hash(it)
        cur.execute("INSERT INTO snapshots (run_id, item_id, key, hash) VALUES (?, ?, ?, ?)",
                    (run_id, it["id"], k, h))
    conn.commit()
    conn.close()
    return run_id

def get_last_two_runs():
    conn = db()
    cur = conn.cursor()
    rows = cur.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 2").fetchall()
    conn.close()
    return rows

def diff_runs(new_run_id: int, old_run_id):
    conn = db()
    cur = conn.cursor()

    new_rows = cur.execute("SELECT key, hash, item_id FROM snapshots WHERE run_id=?", (new_run_id,)).fetchall()
    new_map = {r["key"]: (r["hash"], r["item_id"]) for r in new_rows}

    old_map = {}
    if old_run_id:
        old_rows = cur.execute("SELECT key, hash, item_id FROM snapshots WHERE run_id=?", (old_run_id,)).fetchall()
        old_map = {r["key"]: (r["hash"], r["item_id"]) for r in old_rows}

    changes = {}
    new_keys = set(new_map.keys())
    old_keys = set(old_map.keys())

    for k in new_keys - old_keys:
        changes[new_map[k][1]] = "NEW"
    for k in old_keys - new_keys:
        changes[old_map[k][1]] = "REMOVED"
    for k in new_keys & old_keys:
        new_hash, new_item_id = new_map[k]
        old_hash, _old_item_id = old_map[k]
        changes[new_item_id] = "UPDATED" if new_hash != old_hash else "UNCHANGED"

    items = cur.execute("SELECT * FROM items ORDER BY stage, city, address").fetchall()
    summary = {"NEW": 0, "REMOVED": 0, "UPDATED": 0, "UNCHANGED": 0}
    for it in items:
        ct = changes.get(it["id"], "UNCHANGED")
        if ct in summary:
            summary[ct] += 1

    conn.close()
    return items, changes, summary

# ---------------------------
# Assessor / APN resolution
# ---------------------------

def apn_digits(apn: str) -> str:
    return re.sub(r"[^0-9]", "", apn or "")

def assessor_parcel_url(apn: str) -> str:
    # Humboldt assessor parcel viewer pattern (digits only)
    d = apn_digits(apn)
    return f"https://humboldt-search.gsacorp.io/parcel/{d}"

def fetch_situs_from_assessor(apn: str) -> str | None:
    """
    Pulls the "Location ..." line from the assessor parcel page.
    Returns a FULL address string like: "1265 S BRIDGE ST, Winnemucca, NV 89445"
    Only returns if it contains a street number.
    """
    url = assessor_parcel_url(apn)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; HumboldtDistressMVP/1.0)"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Looks for a line like: "Location 1265 S BRIDGE ST"
    m = re.search(r"(?im)^Location\s+(.+)$", text)
    if not m:
        return None

    loc = m.group(1).strip()
    # Must have a street number, otherwise it's not a situs address
    if not re.search(r"\b\d{1,6}\b", loc):
        return None

    # Normalize to full address
    upper = loc.upper()
    if "WINNEMUCCA" not in upper:
        loc = f"{loc}, Winnemucca"
    if "NV" not in upper:
        loc = f"{loc}, NV"
    if not re.search(r"\b89445\b", loc):
        loc = f"{loc} 89445"

    return loc

def best_address_for_links(it) -> str | None:
    """
    Prefer resolved_situs. Otherwise use address+city/state/zip ONLY if it looks like a street address.
    """
    situs = (it["resolved_situs"] or "").strip()
    if situs and re.search(r"\b\d{1,6}\b", situs):
        return situs

    addr = (it["address"] or "").strip()
    if addr and re.search(r"\b\d{1,6}\b", addr):
        q = f"{addr}, {it['city']}, {it['state']} {it['zip'] or ''}".strip()
        return q

    return None

# ---------------------------
# Link builders (FIXED)
# ---------------------------

def maps_url_for_item(it) -> str:
    """
    Apple Maps deep link. Opens the Maps app directly.
    If no real street address exists, go to assessor parcel page.
    """
    q = best_address_for_links(it)
    if q:
        return f"maps://?q={quote_plus(q)}"

    apn = (it["apn"] or "").strip()
    if apn:
        return assessor_parcel_url(apn)

    return "maps://?q=" + quote_plus(f"{it['city']}, {it['state']}")

def zillow_url_for_item(it) -> str | None:
    """
    Zillow ONLY when there is a real street address.
    Prevents Zillow from dumping you into some random last-used city.
    """
    q = best_address_for_links(it)
    if not q:
        return None
    return f"https://www.zillow.com/homes/{quote_plus(q)}_rb/"

# ---------------------------
# Flask app
# ---------------------------

app = Flask(__name__)
app.secret_key = "dev-secret-change-me"

@app.before_request
def _boot():
    init_db()
    seed_tax_examples()

@app.route("/")
def index():
    runs = get_last_two_runs()
    latest = runs[0] if len(runs) > 0 else None
    prev = runs[1] if len(runs) > 1 else None

    items, changes, summary = ([], {}, {"NEW": 0, "REMOVED": 0, "UPDATED": 0, "UNCHANGED": 0})
    if latest:
        items, changes, summary = diff_runs(latest["id"], prev["id"] if prev else None)

    return render_template(
        "index.html",
        runs=runs, latest=latest, prev=prev,
        items=items, changes=changes, summary=summary,
        maps_url_for_item=maps_url_for_item,
        zillow_url_for_item=zillow_url_for_item,
        assessor_parcel_url=assessor_parcel_url
    )

@app.route("/run", methods=["POST"])
def run_now():
    run_id = create_run_and_snapshot()
    flash(f"Created run #{run_id}.", "success")
    return redirect(url_for("index"))

@app.route("/resolve_apns", methods=["POST"])
def resolve_apns():
    """
    Resolves situs address for items that have APNs (Tax Delinquency tab mainly).
    Stores resolved_situs + assessor_url.
    """
    conn = db()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT id, apn, resolved_situs FROM items
        WHERE apn IS NOT NULL AND TRIM(apn) != ''
        ORDER BY id
    """).fetchall()

    updated = 0
    checked = 0

    for r in rows:
        checked += 1
        apn = (r["apn"] or "").strip()
        url = assessor_parcel_url(apn)

        try:
            situs = fetch_situs_from_assessor(apn)
        except Exception:
            situs = None

        # Always store assessor_url; situs only if found
        if situs and situs != (r["resolved_situs"] or ""):
            cur.execute("UPDATE items SET resolved_situs=?, assessor_url=? WHERE id=?", (situs, url, r["id"]))
            updated += 1
        else:
            # ensure assessor url is set
            cur.execute("UPDATE items SET assessor_url=? WHERE id=?", (url, r["id"]))

    conn.commit()
    conn.close()

    flash(f"Resolved APNs checked={checked}, updated_situs={updated}.", "success")
    return redirect(url_for("index"))

@app.route("/import", methods=["GET", "POST"])
def import_csv():
    if request.method == "GET":
        return render_template("import.html", stages=STAGES)

    file = request.files.get("file")
    if not file:
        flash("No file uploaded.", "danger")
        return redirect(url_for("import_csv"))

    text = file.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        flash("CSV has no header row.", "danger")
        return redirect(url_for("import_csv"))

    header_map = {h: h.lower().strip() for h in reader.fieldnames}
    lowered = [h.lower().strip() for h in reader.fieldnames]
    required = ["stage", "address", "city", "state"]
    if not all(r in lowered for r in required):
        flash("CSV missing required headers: stage,address,city,state", "danger")
        return redirect(url_for("import_csv"))

    conn = db()
    cur = conn.cursor()
    inserted = 0
    allowed = {s for s, _ in STAGES}

    for row in reader:
        r = {header_map.get(k, k).lower().strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        stage = (r.get("stage") or "OTHER").upper().strip()
        if stage not in allowed:
            stage = "OTHER"

        cur.execute(
            """INSERT INTO items (stage, apn, address, city, state, zip, record_date, doc_type, source_url, resolved_situs, assessor_url)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                stage,
                (r.get("apn") or None),
                r["address"],
                r["city"],
                r["state"],
                (r.get("zip") or None),
                (r.get("record_date") or None),
                (r.get("doc_type") or None),
                (r.get("source_url") or None),
                None,
                None,
            )
        )
        inserted += 1

    conn.commit()
    conn.close()
    flash(f"Imported {inserted} rows. Now click Run Search.", "success")
    return redirect(url_for("index"))

@app.route("/reset", methods=["POST"])
def reset_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    flash("Database reset. Reloading seed data.", "warning")
    return redirect(url_for("index"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
