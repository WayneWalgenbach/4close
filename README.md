# Humboldt Distress Listings MVP (Winnemucca / Humboldt County, NV)

Local-first MVP website that:
- tracks distressed-property listings by **run** (snapshot)
- highlights **NEW** and **REMOVED** items since the prior run (yellow badges)
- provides tabs for:
  - **Pre-Foreclosure** (Recorder imports)
  - **Foreclosure / Sale / REO** (Recorder imports)
  - **Tax Delinquency** (seeded from county delinquent tax sale parcel list examples)
- generates one-tap links:
  - Google Maps pin
  - Zillow search (no scraping)
  - Source link (optional)

## Quick start
```bash
cd humboldt_distress_mvp
python -m venv .venv
source .venv/bin/activate   # mac/linux
pip install -r requirements.txt
python app.py
```
Open http://127.0.0.1:5000

## Import Recorder results
Upload a CSV with headers (case-insensitive, extra columns OK):
`stage, apn, address, city, state, zip, record_date, doc_type, source_url`

Stages:
- PRE_FORECLOSURE
- FORECLOSURE_SALE
- REO
- TAX_DELINQUENCY
- OTHER
