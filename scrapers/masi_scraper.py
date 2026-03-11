"""
MASI Index Scraper - Investing.com
Scrapes the last 18 days (17 + current) of MASI index historical data
and stores them into md.market_index (PostgreSQL).
"""
import os
import time
import logging
import certifi
import re
from datetime import datetime

from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ----- SSL Fix -----
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

# ----- CONFIG -----
INDEX_NAME = "MASI"
SOURCE_NAME = "investing"
URL = "https://fr.investing.com/indices/masi-historical-data"
MAX_ROWS = 18  # 17 days + current day

logging.basicConfig(
    filename="masi_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123456@localhost:5432/PFE")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


# ----- WEBDRIVER -----
def get_chrome_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    # Accept French locale for Investing.com
    options.add_argument("--lang=fr")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# ----- NUMBER PARSING -----
def clean_number(text_val):
    """
    Parses numbers in French/European format:
    e.g. '16.902,46' -> 16902.46
    e.g. '1,23M' -> 1230000.0, '54,23K' -> 54230.0
    e.g. '2,23%' -> 2.23
    """
    if not text_val or text_val.strip() in ("-", "", "N/A"):
        return None

    s = text_val.strip()

    # Remove percentage sign
    s = s.replace("%", "").strip()

    # Handle M (millions) and K (thousands) suffixes
    multiplier = 1
    if s.endswith("M"):
        multiplier = 1_000_000
        s = s[:-1]
    elif s.endswith("K"):
        multiplier = 1_000
        s = s[:-1]
    elif s.endswith("B"):
        multiplier = 1_000_000_000
        s = s[:-1]

    # European format: dots are thousands separators, comma is decimal
    # But only strip dots if there's also a comma (confirming European format)
    # or if dot is clearly a thousands separator (groups of 3 digits after it)
    if "," in s and "." in s:
        # Full European format: 16.902,46 -> 16902.46
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Only comma, treat as decimal: 2,23 -> 2.23
        s = s.replace(",", ".")
    # else: no comma, keep dots as-is (unlikely for this source)

    try:
        return float(s) * multiplier
    except ValueError:
        return None


def parse_date(date_str):
    """
    Parses dates in DD/MM/YYYY format.
    e.g. '10/03/2026' -> date(2026, 3, 10)
    """
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


# ----- SCRAPING -----
def scrape_masi():
    driver = None
    try:
        print(f"🚀 Starting MASI Index Scraper (Investing.com)...")
        driver = get_chrome_driver()
        driver.get(URL)

        # Wait for the historical data table to load
        print("   [+] Waiting for table to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        time.sleep(2)  # extra wait for JS rendering

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # Find the data table - on Investing.com it's usually the main table
        table = soup.find("table")
        if not table:
            print("   ❌ Could not find the data table!")
            return

        # Parse rows
        rows = table.find("tbody")
        if not rows:
            print("   ❌ Could not find table body!")
            return

        all_rows = rows.find_all("tr")
        print(f"   [+] Found {len(all_rows)} rows in table. Taking first {MAX_ROWS}...")

        records = []
        for i, row in enumerate(all_rows[:MAX_ROWS]):
            cells = row.find_all("td")
            if len(cells) < 7:
                continue

            # Debug: print raw cell text for first row
            if i == 0:
                print(f"   [DEBUG] Raw cells: {[c.text.strip() for c in cells]}")

            date_val = parse_date(cells[0].text.strip())
            close_val = clean_number(cells[1].text)
            open_val = clean_number(cells[2].text)
            high_val = clean_number(cells[3].text)
            low_val = clean_number(cells[4].text)
            vol_val = clean_number(cells[5].text)
            change_val = clean_number(cells[6].text)

            if date_val is None:
                print(f"   ⚠️ Skipping row with unparseable date: {cells[0].text}")
                continue

            records.append({
                "index_name": INDEX_NAME,
                "trade_date": date_val,
                "close_price": close_val,
                "open_price": open_val,
                "high": high_val,
                "low": low_val,
                "volume": vol_val,
                "change_pct": change_val,
                "source_name": SOURCE_NAME,
            })

        if not records:
            print("   ❌ No valid records parsed!")
            return

        # Print what we found
        print(f"\n   📊 Parsed {len(records)} MASI records:")
        for r in records:
            print(f"      {r['trade_date']} | Close: {r['close_price']} | Vol: {r['volume']} | Chg: {r['change_pct']}%")

        # Insert into DB
        print("\n   [+] Upserting into md.market_index...")
        inserted = 0
        with engine.begin() as conn:
            for r in records:
                res = conn.execute(
                    text("""
                        INSERT INTO md.market_index 
                            (index_name, trade_date, close_price, open_price, high, low, volume, change_pct, source_name)
                        VALUES 
                            (:index_name, :trade_date, :close_price, :open_price, :high, :low, :volume, :change_pct, :source_name)
                        ON CONFLICT (index_name, trade_date) DO UPDATE SET
                            close_price = EXCLUDED.close_price,
                            open_price = EXCLUDED.open_price,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            volume = EXCLUDED.volume,
                            change_pct = EXCLUDED.change_pct,
                            scraped_at = CURRENT_TIMESTAMP
                    """),
                    r,
                )
                inserted += res.rowcount

        print(f"   ✅ Upserted {inserted} MASI records into md.market_index!")

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        logging.error(f"MASI scraping failed: {e}")
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    scrape_masi()
