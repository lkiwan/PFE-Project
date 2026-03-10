import os
import time
import logging
from io import StringIO
from datetime import datetime, time as dt_time, timedelta
from typing import Optional, Dict

import certifi
import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
SOURCE_NAME = "medias24"
BASE_URL_HIST = "https://medias24.com/leboursier/fiche-action?action={slug}&valeur=historiques"
BASE_URL_OB = "https://medias24.com/leboursier/fiche-action?action={slug}&valeur=carnet-d-ordres"

# The target markets the user wants:
TARGET_MARKETS = {
    "AKDITAL": "aktidal", 
    "ALLIANCES": "alliances-p",
    "ATW": "attijariwafa-bank",
    "CIH": "cih-p",
    "ADDOHA-P": "addoha-p", 
    "IAM": "maroc-telecom",
    "JET CONTRACTORS": "jet-contractors-p",
    "SGTM": "sgtm-p",
    "SODEP": "sodep-p",
    "TAQA": "taqa-morocco-p",
    "TGCC": "tgcc",
}

logging.basicConfig(
    filename="medias24_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123456@localhost:5432/PFE")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# -----------------------------------------------------------------------------
# SELENIUM
# -----------------------------------------------------------------------------
def get_chrome_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def clean_number(value) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s == "-":
        return None

    s = s.replace("\u202f", "").replace(" ", "").replace("%", "")
    s = s.replace("MAD", "").replace("mad", "").replace("Mad", "")
    s = s.replace(",", ".")
    s = s.replace("\xa0", "")

    try:
        return float(s)
    except ValueError:
        return None

def scrape_history(driver, slug: str, instrument_id: int):
    url = BASE_URL_HIST.format(slug=slug)
    driver.get(url)
    try:
        df = None
        # Custom retry loop since Medias24 dynamically loads this table asynchronously
        for _ in range(15):
            time.sleep(2)
            tables = pd.read_html(StringIO(driver.page_source), decimal=",", thousands=" ")
            if tables:
                for table in tables:
                    cols_str = " ".join([str(c).lower() for c in table.columns])
                    if "cours" in cols_str and ("date" in cols_str or "séance" in cols_str):
                        df = table
                        break
                if df is not None and len(df) > 0:
                    break
        
        if df is None or df.empty:
            logging.warning(f"No tables found for history slug: {slug}")
            return []
            
        # Standard Medias24 structure: ['Date', 'Cours', 'Variation %', ...]
        
        # Take the top 18 rows (today + 17 days of history)
        df = df.head(18)
        
        def val(keys):
            for k in keys:
                if k in row and pd.notna(row.get(k)):
                    return clean_number(row[k])
            return None
            
        payload = []
        # Find the actual date column name
        date_col = next((c for c in df.columns if 'date' in str(c).lower() or 'séance' in str(c).lower()), 'Date')
        
        for index, row in df.iterrows():
            date_str = str(row.get(date_col, ''))
            
            try:
                # Expected format: DD/MM/YYYY
                trade_date = datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
            except ValueError:
                continue
                
            price = val(['Cours', 'COURS'])
            change_pct = val(['Variation %', 'VARIATION %', 'Variation', 'VARIATION'])
            high = val(['+Haut', 'PLUS HAUT', 'Plus haut'])
            low = val(['+Bas', 'PLUS BAS', 'Plus bas'])
            open_price = val(['Ouverture', 'OUVERTURE'])
            volume = val(['Volume', 'VOLUME'])
            
            payload.append({
                "instrument_id": instrument_id,
                "trade_date": trade_date,
                "price": price,
                "open": open_price,
                "high": high,
                "low": low,
                "volume": volume,
                "change_pct": change_pct,
                "source_name": SOURCE_NAME,
                "scraped_at": datetime.now()
            })
            
        return payload
    except Exception as e:
        logging.error(f"Error scraping history via Selenium for {slug}: {e}")
        return []

def scrape_orderbook(driver, slug: str, instrument_id: int):
    url = BASE_URL_OB.format(slug=slug)
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )
        time.sleep(2)
        
        tables = pd.read_html(StringIO(driver.page_source), decimal=",", thousands=" ")
        
        # We need to find the table that has 'Achat' and 'Vente'
        target_df = None
        for df in tables:
            cols_str = " ".join([str(c).lower() for c in df.columns])
            if "achat" in cols_str or "vente" in cols_str or "prix" in cols_str:
                target_df = df
                break
                
        if target_df is None or target_df.empty:
            logging.warning(f"No orderbook table found for {slug}")
            return None
            
        # Standardize columns
        if len(target_df.columns) >= 6:
            target_df = target_df.iloc[:, :6]
            target_df.columns = ["ordres_achat", "qte_achat", "prix_achat", "prix_vente", "qte_vente", "ordres_vente"]
        elif len(target_df.columns) == 4:
            target_df.columns = ["qte_achat", "prix_achat", "prix_vente", "qte_vente"]
            
        if target_df.empty:
            return []
            
        payloads = []
        snap_time = datetime.now()
        for _, row in target_df.iterrows():
            # Skip the 'TOTAL' row
            if "total" in str(row.get("prix_achat", "")).lower() or "total" in str(row.get("prix_vente", "")).lower():
                continue
                
            bid_qty = clean_number(row.get("qte_achat"))
            bid_price = clean_number(row.get("prix_achat"))
            ask_price = clean_number(row.get("prix_vente"))
            ask_qty = clean_number(row.get("qte_vente"))
            
            if bid_price is None and ask_price is None and bid_qty is None and ask_qty is None:
                continue

            snap_time += timedelta(microseconds=1)

            payloads.append({
                "instrument_id": instrument_id,
                "snapshot_time": snap_time,
                "bid_price": bid_price,
                "bid_qty": bid_qty,
                "ask_price": ask_price,
                "ask_qty": ask_qty,
                # Order books table doesn't have ordres_achat and ordres_vente
                "source_name": SOURCE_NAME,
            })

        return payloads
    except Exception as e:
        logging.error(f"Error scraping orderbook Selenium for {slug}: {e}")
        return None

def upsert_history_to_db(conn, payloads):
    if not payloads:
        return 0
    stmt = text("""
        INSERT INTO md.eod_bars (
            instrument_id, trade_date, price, open, high, low, volume, change_pct, source_name, scraped_at
        )
        VALUES (
            :instrument_id, :trade_date, :price, :open, :high, :low, :volume, :change_pct, :source_name, :scraped_at
        )
        ON CONFLICT (instrument_id, trade_date)
        DO UPDATE SET
            price = EXCLUDED.price,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            volume = EXCLUDED.volume,
            change_pct = EXCLUDED.change_pct,
            source_name = COALESCE(md.eod_bars.source_name, EXCLUDED.source_name),
            scraped_at = EXCLUDED.scraped_at
    """)
    conn.execute(stmt, payloads)
    return len(payloads)

def insert_orderbook_to_db(conn, payloads):
    if not payloads:
        return 0
    # The order_books table doesn't enforce uniqueness on snapshots, so we blindly insert
    stmt = text("""
        INSERT INTO md.order_books (
            instrument_id, snapshot_time, bid_price, bid_qty, ask_price, ask_qty
        )
        VALUES (
            :instrument_id, :snapshot_time, :bid_price, :bid_qty, :ask_price, :ask_qty
        )
    """)
    conn.execute(stmt, payloads)
    return len(payloads)

def get_instrument_ids(conn) -> Dict[str, int]:
    symbols = list(TARGET_MARKETS.keys())
    rows = conn.execute(
        text("SELECT instrument_id, symbol FROM ref.instruments WHERE symbol = ANY(:symbols)"),
        {"symbols": symbols}
    ).mappings().all()
    return {r["symbol"]: r["instrument_id"] for r in rows}

def main():
    print("🚀 Starting Medias24 Scraper for Historical & Orderbook Data...")
    driver = None
    try:
        driver = get_chrome_driver()
        
        with engine.begin() as conn:
            instrument_map = get_instrument_ids(conn)
            
            for symbol, slug in TARGET_MARKETS.items():
                instrument_id = instrument_map.get(symbol)
                if not instrument_id:
                    print(f"⚠️ Skipping {symbol} ({slug}): Intstrument ID not found in ref.instruments!")
                    continue
                    
                print(f"\nProcessing {symbol} ({slug}) [ID: {instrument_id}]...")
                
                # 1. Scrape 18 days of History
                print(f"   [+] Scraping History...")
                history_payload = scrape_history(driver, slug, instrument_id)
                if history_payload:
                    inserted_hist = upsert_history_to_db(conn, history_payload)
                    print(f"       ✅ Upserted {inserted_hist} history records for {symbol} to DB")
                else:
                    print(f"       ❌ Failed to get history for {symbol}")

                # 2. Scrape Orderbook (All levels)
                print(f"   [+] Scraping Orderbook...")
                ob_payloads = scrape_orderbook(driver, slug, instrument_id)
                if ob_payloads:
                    inserted_ob = insert_orderbook_to_db(conn, ob_payloads)
                    print(f"       ✅ Inserted {inserted_ob} orderbook rows for {symbol} to DB")
                else:
                    print(f"       ❌ Failed to get orderbook for {symbol}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        logging.exception("Critical error in main scraper:")
    finally:
        if driver:
            driver.quit()
        print("\n✅ Scraping finished.")


if __name__ == "__main__":
    main()
