import csv
import io
import os
import re
import sqlite3
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
import pdfplumber

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "data", "app.db")

# Humboldt County Tax Auction parcel list (April 2026)
TAX_LIST_PDF = "https://www.humboldtcountynv.gov/DocumentCenter/View/8386/2026-List-of-Parcels"

# Humboldt County Assessor (APN -> parcel detail)
ASSESSOR_BASE = "https://humboldt-search.gsacorp.io"

STAGES = [
    ("PRE_FORECLOSURE", "Pre-Foreclosure"),
    ("FORECLOSURE_SALE", "Foreclosure / Sale"),
    ("REO", "REO / Bank-Owned"),
    ("TAX_DELINQUENCY", "Tax Delinquency (Auction)"),
    ("OTHER", "Other"),
]

app = Flask(__name__, template_folder="templates")
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")


# -----------------------
# DB
# -----------------------
def db():
    os.makedirs(os.path.join(APP_DIR, "data"), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_cols(cur, table):
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.executescript(
        """
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
        """
    )

    # Lightweight migration: add columns if missing
    cols = _table_cols(cur, "items")
    if "assessor_url" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN assessor_url TEXT")
    if "resolved_situs" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN resolved_situs TEXT")
    if "resolved_at" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN resolved_at TEXT")

    conn.commit()
    conn.close()


@app.before_request
def _boot():
    init_db()


# -----------------------
# Key/Hash for snapshot diffs
# -----------------------
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
    # include resolved_situs so "Resolve APNs" can change items and show UPDATED
    parts = [
        row["stage"],
        row["apn"] or "",
        row["address"],
        row["city"],
        row["state"],
        row["zip"] or "",
        row["record_date"] or "",
        row["doc_type"] or "",
        row["source_url"] or "",
        row.get("assessor_url") or "",
        row.get("resolved_situs") or "",
    ]
    return "|".join([norm(p) for p in parts])


# -----------------------
# Maps/Zillow link builders
# -----------------------
def comgooglemaps_url(query: str) -> str:
    # iPhone deep link to open Google Maps app directly
    return f"comgooglemaps://?q={quote_plus(query)}"


def web_maps_url(query: str) -> str:
    # fallback web link
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(query)}"


def normalize_apn(apn: str) -> str:
    return re.sub(r"\D", "", (apn or "").strip())


def assessor_parcel_url(apn: str) -> str:
    digits = normalize_apn(apn)
    return f"{ASSESSOR_BASE}/parcel/{digits}" if digits else ASSESSOR_BASE


def maps_url_for_item(it) -> str:
    """
    Priority:
    1) resolved_situs -> open Google Maps app directly
    2) street-like address -> open Google Maps app directly
    3) APN -> open assessor parcel page (accurate, not a fake pin)
    """
    situs = (it["resolved_situs"] or "").strip()
    if situs:
        return comgooglemaps_url(situs)

    addr = (it["address"] or "").strip()
    if re.search(r"\b\d{1,6}\b", addr) and len(addr) >= 8:
        q = f"{addr}, {it['city']}, {it['state']} {it['zip'] or ''}".strip()
        return comgooglemaps_url(q)

    apn = (it["apn"] or "").strip()
    if apn:
        return assessor_parcel_url(apn)

    q = f"{addr}, {it['city']}, {it['state']}".strip()
    return comgooglemaps_url(q)


def zillow_url_for_item(it) -> str:
    # Zillow often works best with a real address if we have it
    q = (it["resolved_situs"] or "").strip()
    if not q:
        q = f"{it['address']}, {it['city']}, {it['state']} {it['zip'] or ''}".strip()
    return f"https://www.zillow.com/homes/{quote_plus(q)}_rb/"


# -----------------------
# Assessor resolver (APN -> situs)
# -----------------------
def resolve_situs_from_assessor(apn: str) -> dict:
    """
    Returns:
      { ok: bool, situs: str, assessor_url: str, error?: str }
    """
    url = assessor_parcel_url(apn)
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return {"ok": False, "situs": "", "assessor_url": url, "error": f"HTTP {r.status_code}"}

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        situs_parts = []
        for i, ln in enumerate(lines):
            if ln == "Location" and i + 1 < len(lines):
                situs_parts.append(lines[i + 1])
                if i + 2 < len(lines) and ("," in lines[i + 2] or "NV" in lines[i + 2]):
                    situs_parts.append(lines[i + 2])
                break
            if ln.startswith("Location "):
                situs_parts.append(ln.replace("Location ", "", 1).strip())
                if i + 1 < len(lines) and ("," in lines[i + 1] or "NV" in lines[i + 1]):
                    situs_parts.append(lines[i + 1])
                break

        situs = ", ".join([p for p in situs_parts if p]).strip()

        # If it's only city/state (no street number), don't pretend it's a precise pin
        if situs and not re.search(r"\b\d{1,6}\b", situs):
            return {"ok": False, "situs": "", "assessor_url": url}

        # add default zip for Winnemucca if missing
        if situs and "WINNEMUCCA" in situs.upper() and not re.search(r"\b\d{5}\b", situs):
            situs = situs + " 89445"

        return {"ok": bool(situs), "situs": situs, "assessor_url": url}
    except Exception as e:
        return {"ok": False, "situs": "", "assessor_url": url, "error": str(e)}


def resolve_all_unresolved(limit: int = 40) -> dict:
    """
    Resolve up to `limit` items that have an APN but no resolved_situs yet.
    """
    conn = db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT id, apn FROM items
        WHERE apn IS NOT NULL AND TRIM(apn) <> ''
          AND (resolved_situs IS NULL OR TRIM(resolved_situs) = '')
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    updated = 0
    skipped = 0

    for r in rows:
        item_id = r["id"]
        apn = r["apn"]
        res = resolve_situs_from_assessor(apn)
        now = datetime.now(timezone.utc).isoformat()

        # always store assessor_url; store situs only if ok
        cur.execute(
            """
            UPDATE items
            SET assessor_url=?, resolved_situs=?, resolved_at=?
            WHERE id=?
            """,
            (res.get("assessor_url"), res.get("situs") if res.get("ok") else "", now, item_id),
        )
        if res.get("ok"):
            updated += 1
        else:
            skipped += 1

    conn.commit()
    conn.close()

    return {"processed": len(rows), "resolved": updated, "no_situs": skipped}


# -----------------------
# Runs / diffs
# -----------------------
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
        cur.execute(
            "INSERT INTO snapshots (run_id, item_id, key, hash) VALUES (?, ?, ?, ?)",
            (run_id, it["id"], k, h),
        )

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

    new_rows = cur.execute(
        "SELECT key, hash, item_id FROM snapshots WHERE run_id=?",
        (new_run_id,),
    ).fetchall()
    new_map = {r["key"]: (r["hash"], r["item_id"]) for r in new_rows}

    old_map = {}
    if old_run_id:
        old_rows = cur.execute(
            "SELECT key, hash, item_id FROM snapshots WHERE run_id=?",
            (old_run_id,),
        ).fetchall()
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
        old_hash, _ = old_map[k]
        changes[new_item_id] = "UPDATED" if new_hash != old_hash else "UNCHANGED"

    items = cur.execute("SELECT * FROM items ORDER BY stage, city, address").fetchall()
    summary = {"NEW": 0, "REMOVED": 0, "UPDATED": 0, "UNCHANGED": 0}
    for it in items:
        ct = changes.get(it["id"], "UNCHANGED")
        summary[ct] += 1

    conn.close()
    return items, changes, summary


# -----------------------
# Tax refresh (April 2026 PDF)
# -----------------------
def parse_tax_pdf_for_apns(pdf_bytes: bytes):
    apn_re = re.compile(r"\b\d{2}-\d{4}-\d{2}\b")
    found = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            found.extend(apn_re.findall(text))
    return sorted(set(found))


def replace_tax_rows(apns):
    """
    Replace TAX_DELINQUENCY items with APN-only rows (then resolver fills situs).
    """
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM items WHERE stage='TAX_DELINQUENCY'")

    for apn in apns:
        cur.execute(
            """
            INSERT INTO items(stage, apn, address, city, state, zip, doc_type, source_url, assessor_url, resolved_situs, resolved_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "TAX_DELINQUENCY",
                apn,
                f"APN {apn} (resolve for situs)",
                "Winnemucca",
                "NV",
                "89445",
                "Parcel List for April 2026 Delinquent Tax Auction",
                TAX_LIST_PDF,
                assessor_parcel_url(apn),
                "",
                None,
            ),
        )

    conn.commit()
    conn.close()


# -----------------------
# Routes
# -----------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/")
def index():
    runs = get_last_two_runs()
    latest = runs[0] if len(runs) > 0 else None
    prev = runs[1] if len(runs) > 1 else None

    items, changes, summary = ([], {}, {"NEW": 0, "REMOVED": 0, "UPDATED": 0, "UNCHANGED": 0})
    if latest:
        items, changes, summary = diff_runs(latest["id"], prev["id"] if prev else None)

    return render_template(
        "index.html",
        latest=latest,
        prev=prev,
        items=items,
        changes=changes,
        summary=summary,
        stages=STAGES,
        maps_url_for_item=maps_url_for_item,
        zillow_url_for_item=zillow_url_for_item,
        assessor_parcel_url=assessor_parcel_url,
    )


@app.post("/run")
def run_now():
    run_id = create_run_and_snapshot()
    flash(f"Created snapshot run #{run_id}.", "success")
    return redirect(url_for("index"))


@app.get("/import")
def import_csv():
    return render_template("import.html", stages=STAGES)


@app.post("/import")
def import_csv_post():
    file = request.files.get("file")
    if not file:
        flash("No file uploaded.", "danger")
        return redirect(url_for("import_csv"))

    text = file.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        flash("CSV missing header row.", "danger")
        return redirect(url_for("import_csv"))

    lowered = [h.lower().strip() for h in reader.fieldnames]
    required = ["stage", "address", "city", "state"]
    if not all(r in lowered for r in required):
        flash("CSV
