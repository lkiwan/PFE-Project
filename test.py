import os
import time
import logging
from io import StringIO
from datetime import datetime, time as dt_time
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
SOURCE_NAME = "casablanca_bourse"
LIVE_MARKET_URL = "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement/"

# Fallback mapping if ref.instrument_aliases is not seeded yet
# raw site name -> symbol in ref.instruments
FALLBACK_NAME_TO_SYMBOL = {
    "AKDITAL": "AKDITAL",
    "ALLIANCES": "ALLIANCES",
    "ATTIJARIWAFA BANK": "ATW",
    "CIH": "CIH",
    "DOUJA PROM ADDOHA": "ADDOUHA",
    "ITISSALAT AL-MAGHRIB": "IAM",
    "JET CONTRACTORS": "JET CONTRACTORS",
    "SGTM S.A": "SGTM",
    "SODEP-Marsa Maroc": "SODEP",
    "TAQA MOROCCO": "TAQA",
    "TGCC S.A": "TGCC",
}

CHECK_EVERY_SECONDS = 300
RUN_AFTER_MARKET_CLOSE = dt_time(15, 40)

logging.basicConfig(
    filename="trading_bot_lvl1.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()


# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing DATABASE_URL environment variable")
    return create_engine(db_url, pool_pre_ping=True, future=True)


engine = get_engine()


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
    s = s.replace(",", ".")
    s = s.replace("\xa0", "")

    try:
        return float(s)
    except ValueError:
        return None


def pick_column(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def load_alias_map(conn) -> Dict[str, int]:
    rows = conn.execute(
        text("""
            SELECT alias_value, instrument_id
            FROM ref.instrument_aliases
            WHERE source_name = :source_name
        """),
        {"source_name": SOURCE_NAME},
    ).mappings().all()

    return {row["alias_value"]: row["instrument_id"] for row in rows}


def get_instrument_id_by_symbol(conn, symbol: str) -> Optional[int]:
    return conn.execute(
        text("""
            SELECT instrument_id
            FROM ref.instruments
            WHERE symbol = :symbol
        """),
        {"symbol": symbol},
    ).scalar_one_or_none()


def parse_live_market_html(page_source: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(page_source), decimal=",", thousands=" ")
    if not tables:
        raise RuntimeError("No tables found on CSE live market page")

    df = pd.concat(tables, ignore_index=True)

    instrument_col = pick_column(df, ["Instrument", "Valeur", "Libellé", "Libelle"])
    if instrument_col is None:
        raise RuntimeError(f"Instrument column not found. Columns: {df.columns.tolist()}")

    price_col = pick_column(df, ["Dernier cours", "Price", "Cours"])
    open_col = pick_column(df, ["Ouverture", "Open"])
    high_col = pick_column(df, ["Plus haut", "High"])
    low_col = pick_column(df, ["Plus bas", "Low"])
    volume_col = pick_column(df, ["Quantité échangée", "Vol.", "Volume titres", "Volume"])
    change_col = pick_column(df, ["Variation en %", "Change %", "Variation %"])

    out = pd.DataFrame()
    out["raw_name"] = df[instrument_col].astype(str).str.strip()
    out["trade_date"] = datetime.now().date()
    out["price"] = df[price_col].apply(clean_number) if price_col else None
    out["open"] = df[open_col].apply(clean_number) if open_col else None
    out["high"] = df[high_col].apply(clean_number) if high_col else None
    out["low"] = df[low_col].apply(clean_number) if low_col else None
    out["volume"] = df[volume_col].apply(clean_number) if volume_col else None
    out["change_pct"] = df[change_col].apply(clean_number) if change_col else None

    out = out.dropna(subset=["raw_name"])
    out = out[out["raw_name"].isin(FALLBACK_NAME_TO_SYMBOL.keys())].copy()
    out = out.drop_duplicates(subset=["raw_name", "trade_date"])
    return out


def upsert_eod_rows(conn, rows):
    if not rows:
        return 0

    stmt = text("""
        INSERT INTO md.eod_bars (
            instrument_id,
            trade_date,
            price,
            open,
            high,
            low,
            volume,
            change_pct,
            source_name,
            scraped_at
        )
        VALUES (
            :instrument_id,
            :trade_date,
            :price,
            :open,
            :high,
            :low,
            :volume,
            :change_pct,
            :source_name,
            now()
        )
        ON CONFLICT (instrument_id, trade_date)
        DO UPDATE SET
            price = EXCLUDED.price,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            volume = EXCLUDED.volume,
            change_pct = EXCLUDED.change_pct,
            source_name = EXCLUDED.source_name,
            scraped_at = now()
    """)
    conn.execute(stmt, rows)
    return len(rows)


# -----------------------------------------------------------------------------
# MAIN SCRAPER
# -----------------------------------------------------------------------------
def scrape_level_1_once():
    logging.info("Level 1: starting EOD scrape")
    driver = None

    try:
        driver = get_chrome_driver()
        driver.get(LIVE_MARKET_URL)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )
        time.sleep(2)

        df = parse_live_market_html(driver.page_source)
        logging.info("Level 1: parsed %s candidate rows", len(df))

        with engine.begin() as conn:
            alias_map = load_alias_map(conn)
            payload = []

            for row in df.to_dict(orient="records"):
                raw_name = row["raw_name"]

                instrument_id = alias_map.get(raw_name)
                if instrument_id is None:
                    symbol = FALLBACK_NAME_TO_SYMBOL.get(raw_name)
                    if symbol:
                        instrument_id = get_instrument_id_by_symbol(conn, symbol)

                if instrument_id is None:
                    logging.warning("Level 1: instrument not found for raw_name='%s'", raw_name)
                    continue

                payload.append({
                    "instrument_id": instrument_id,
                    "trade_date": row["trade_date"],
                    "price": row["price"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "volume": row["volume"],
                    "change_pct": row["change_pct"],
                    "source_name": SOURCE_NAME,
                })

            inserted = upsert_eod_rows(conn, payload)
            logging.info("Level 1: upserted %s EOD rows", inserted)
            print(f"✅ Level 1 done: {inserted} rows upserted")

    except Exception as exc:
        logging.exception("Level 1 error: %s", exc)
        print(f"❌ Level 1 error: {exc}")
    finally:
        if driver:
            driver.quit()


def should_run_eod(now_dt: datetime, last_run_date: Optional[datetime.date]) -> bool:
    if now_dt.weekday() >= 5:
        return False
    if now_dt.time() < RUN_AFTER_MARKET_CLOSE:
        return False
    if last_run_date == now_dt.date():
        return False
    return True


def run_service():
    print("🐳 Level 1 EOD service started")
    logging.info("Level 1 service started")
    last_run_date = None

    while True:
        now_dt = datetime.now()
        try:
            if should_run_eod(now_dt, last_run_date):
                scrape_level_1_once()
                last_run_date = now_dt.date()
            else:
                logging.info("Level 1 idle - waiting for market close")
        except Exception as exc:
            logging.exception("Level 1 service loop error: %s", exc)

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    run_mode = os.getenv("RUN_MODE", "service").lower()
    if run_mode == "once":
        scrape_level_1_once()
    else:
        run_service()