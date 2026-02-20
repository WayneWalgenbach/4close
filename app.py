import csv
import io
import os
import sqlite3
import json
from datetime import datetime, timezone
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
        row["zip"] or "", row["record_date"] or "", row["doc_type"] or "", row["source_url"] or ""
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

def maps_url(address, city, state, zip_):
    from urllib.parse import quote_plus
    q = f"{address}, {city}, {state} {zip_ or ''}".strip()
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(q)}"

def zillow_url(address, city, state, zip_):
    from urllib.parse import quote_plus
    q = f"{address}, {city}, {state} {zip_ or ''}".strip()
    return f"https://www.zillow.com/homes/{quote_plus(q)}_rb/"

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
    items, changes, summary = ([], {}, {"NEW":0, "REMOVED":0, "UPDATED":0, "UNCHANGED":0})
    if latest:
        items, changes, summary = diff_runs(latest["id"], prev["id"] if prev else None)
    return render_template("index.html", runs=runs, latest=latest, prev=prev,
                           items=items, changes=changes, summary=summary,
                           maps_url=maps_url, zillow_url=zillow_url)

@app.route("/run", methods=["POST"])
def run_now():
    run_id = create_run_and_snapshot()
    flash(f"Created run #{run_id}.", "success")
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
            """INSERT INTO items (stage, apn, address, city, state, zip, record_date, doc_type, source_url)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
    import os
    port = int(os.environ.get('PORT', '5000'))
    app.run(host='0.0.0.0', port=port, debug=False)
