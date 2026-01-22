import json
import re
from datetime import datetime, timedelta, timezone

from playwright.sync_api import sync_playwright


BASE = "https://www.nevadapublicnotice.com/"
SEARCH_URL = "https://www.nevadapublicnotice.com/Search.aspx"


def guess_address(text: str) -> str | None:
    """
    Very lightweight address extraction from notice text.
    It won't be perfect, but it catches many "Property Address: ..." patterns.
    """
    patterns = [
        r"Property Address:\s*(.+?)(?:\n|$)",
        r"PROPERTY ADDRESS:\s*(.+?)(?:\n|$)",
        r"Site Address:\s*(.+?)(?:\n|$)",
        r"\b(\d{2,6}\s+[A-Za-z0-9.\-'\s]+,\s*Winnemucca,\s*NV\s*\d{5})\b",
    ]
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip(" .")
    return None


def main():
    now = datetime.now(timezone.utc)
    out = {
        "generated_utc": now.isoformat(),
        "source": BASE,
        "query": {
            "keywords": "Notice of Trustee Sale",
            "county": "Humboldt",
            "city": "Winnemucca",
        },
        "items": [],
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Open search page
        page.goto(SEARCH_URL, wait_until="networkidle")

        # --- Fill search UI (selectors are intentionally resilient) ---
        # Keywords
        try:
            page.get_by_label(re.compile(r"Search Keywords", re.I)).fill("Notice of Trustee Sale")
        except Exception:
            # fallback: first textbox on page
            page.locator("input[type='text']").first.fill("Notice of Trustee Sale")

        # County = Humboldt
        try:
            page.locator("select").filter(has_text=re.compile("County", re.I)).first.select_option(label="Humboldt")
        except Exception:
            # fallback: choose Humboldt from any select
            page.locator("select").first.select_option(label="Humboldt")

        # City/Town = Winnemucca (optional but useful)
        try:
            page.locator("select").filter(has_text=re.compile("City", re.I)).first.select_option(label="Winnemucca")
        except Exception:
            pass

        # Date range: last 90 days (site supports date filtering; UI varies)
        # If the UI doesn't expose date boxes cleanly, we still get recent notices by default.
        # We keep this simple to avoid brittle selectors.

        # Click Search (button text differs; try a few)
        clicked = False
        for name in ["Search", "Submit", "Go"]:
            try:
                page.get_by_role("button", name=re.compile(name, re.I)).click()
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            # fallback: click a likely "Search" input
            page.locator("input[type='submit'], button[type='submit']").first.click()

        page.wait_for_load_state("networkidle")

        # Collect result links. Many ASP.NET notice sites link to a detail page.
        links = page.locator("a[href*='Detail'], a[href*='detail'], a[href*='Notice'], a[href*='Public']").all()

        # If we didn’t find obvious links, dump the page HTML for debugging (stored in Actions logs)
        if not links:
            html = page.content()
            print("NO LINKS FOUND. First 500 chars of HTML:")
            print(html[:500])

        # Visit up to 50 notices per run
        max_items = 50
        seen = set()

        for a in links[: max_items * 2]:  # scan a bit more, de-dupe
            try:
                href = a.get_attribute("href")
                title = (a.inner_text() or "").strip()
            except Exception:
                continue

            if not href:
                continue

            # Make absolute URL
            if href.startswith("/"):
                url = BASE.rstrip("/") + href
            elif href.startswith("http"):
                url = href
            else:
                url = BASE.rstrip("/") + "/" + href.lstrip("/")

            if url in seen:
                continue
            seen.add(url)

            # Open detail page
            page.goto(url, wait_until="networkidle")
            text = page.inner_text("body")

            addr = guess_address(text)

            item = {
                "title": title if title else None,
                "detail_url": url,
                "address_guess": addr,
            }

            out["items"].append(item)

            if len(out["items"]) >= max_items:
                break

        browser.close()

    # Write output for your website to consume
    import os
    os.makedirs("data", exist_ok=True)
    with open("data/preforeclosures.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote data/preforeclosures.json with {len(out['items'])} items")


if __name__ == "__main__":
    main()
