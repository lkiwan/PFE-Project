import os
import time
import logging
from io import StringIO
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Tuple

import certifi
import pandas as pd
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
SOURCE_NAME = "medias24"
BASE_URL = "https://medias24.com/leboursier/fiche-action?action={slug}&valeur=carnet-d-ordres"

# Fallback if ref.instrument_aliases is not seeded yet
# symbol -> medias24 slug
FALLBACK_SLUGS = {
    "AKDITAL": "aktidal",
    "ALLIANCES": "alliances",
    "ATW": "attijariwafa-bank",
    "CIH": "cih",
    "ADDOUHA": "douja-prom-addoha",
    "IAM": "maroc-telecom",
    "JET CONTRACTORS": "jet-contractors",
    "SGTM": "societe-d-equipement-domestique-et-menager",
    "SODEP": "marsa-maroc",
    "TAQA": "taqa-morocco",
    "TGCC": "tgcc-s.a",
}

CHECK_EVERY_SECONDS = 300
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(15, 40)

logging.basicConfig(
    filename="trading_bot_lvl2.log",
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
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def clean_float(value) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s == "-":
        return None
    s = s.replace("\u202f", "").replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def load_symbols_from_db(conn) -> List[Tuple[int, str, str]]:
    rows = conn.execute(
        text("""
            SELECT i.instrument_id, i.symbol, ia.alias_value AS slug
            FROM ref.instruments i
            JOIN ref.instrument_aliases ia
              ON ia.instrument_id = i.instrument_id
            WHERE ia.source_name = :source_name
              AND i.is_active = TRUE
        """),
        {"source_name": SOURCE_NAME},
    ).mappings().all()

    result = []
    for row in rows:
        result.append((row["instrument_id"], row["symbol"], row["slug"]))
    return result


def fallback_symbols_from_db(conn) -> List[Tuple[int, str, str]]:
    symbols = list(FALLBACK_SLUGS.keys())
    rows = conn.execute(
        text("""
            SELECT instrument_id, symbol
            FROM ref.instruments
            WHERE symbol = ANY(:symbols)
              AND is_active = TRUE
        """),
        {"symbols": symbols},
    ).mappings().all()

    result = []
    for row in rows:
        symbol = row["symbol"]
        slug = FALLBACK_SLUGS.get(symbol)
        if slug:
            result.append((row["instrument_id"], symbol, slug))
    return result


def choose_order_book_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    for df in tables:
        cols = [str(c).lower() for c in df.columns]
        joined = " | ".join(cols)
        if len(df.columns) >= 4 and ("achat" in joined or "vente" in joined or "prix" in joined):
            return df
    if tables:
        return tables[0]
    return None


def normalize_order_book_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    if len(df.columns) < 4:
        raise RuntimeError(f"Unexpected order book structure: {df.columns.tolist()}")

    # Try to map common structures
    # Expected shape often:
    # ordres achat | quantité | prix | prix.1 | quantité.1 | ordres vente
    col_count = len(df.columns)

    if col_count >= 6:
        df = df.iloc[:, :6]
        df.columns = [
            "ordres_achat",
            "qte_achat",
            "prix_achat",
            "prix_vente",
            "qte_vente",
            "ordres_vente",
        ]
    elif col_count == 4:
        df.columns = ["qte_achat", "prix_achat", "prix_vente", "qte_vente"]
    else:
        # generic fallback
        renamed = {}
        for idx, col in enumerate(df.columns):
            renamed[col] = f"col_{idx}"
        df = df.rename(columns=renamed)

    return df


def insert_order_book_row(conn, payload: dict):
    conn.execute(
        text("""
            INSERT INTO md.order_books (
                instrument_id,
                snapshot_time,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                source_name,
                scraped_at
            )
            VALUES (
                :instrument_id,
                :snapshot_time,
                :bid_price,
                :bid_qty,
                :ask_price,
                :ask_qty,
                :source_name,
                now()
            )
        """),
        payload,
    )


# -----------------------------------------------------------------------------
# MAIN SCRAPER
# -----------------------------------------------------------------------------
def scrape_iteration(driver):
    logging.info("Level 2: starting iteration")

    with engine.begin() as conn:
        instruments = load_symbols_from_db(conn)
        if not instruments:
            instruments = fallback_symbols_from_db(conn)

        if not instruments:
            raise RuntimeError(
                "No instruments/slugs found. Seed ref.instrument_aliases or ref.instruments first."
            )

        for instrument_id, symbol, slug in instruments:
            page_url = BASE_URL.format(slug=slug)

            try:
                driver.get(page_url)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
                )
                time.sleep(1.5)

                tables = pd.read_html(StringIO(driver.page_source), decimal=",", thousands=" ")
                if not tables:
                    logging.warning("Level 2: no tables found for %s", symbol)
                    continue

                raw_df = choose_order_book_table(tables)
                if raw_df is None or raw_df.empty:
                    logging.warning("Level 2: empty order book for %s", symbol)
                    continue

                df = normalize_order_book_df(raw_df)
                if df.empty:
                    logging.warning("Level 2: normalized table empty for %s", symbol)
                    continue

                best = df.iloc[0].to_dict()

                bid_qty = clean_float(best.get("qte_achat"))
                bid_price = clean_float(best.get("prix_achat"))
                ask_price = clean_float(best.get("prix_vente"))
                ask_qty = clean_float(best.get("qte_vente"))

                if bid_price is None and ask_price is None:
                    logging.warning("Level 2: no valid bid/ask for %s", symbol)
                    continue

                payload = {
                    "instrument_id": instrument_id,
                    "snapshot_time": datetime.now(),
                    "bid_price": bid_price,
                    "bid_qty": bid_qty,
                    "ask_price": ask_price,
                    "ask_qty": ask_qty,
                    "source_name": SOURCE_NAME,
                }

                insert_order_book_row(conn, payload)
                logging.info(
                    "Level 2: %s saved | bid=%s x %s | ask=%s x %s",
                    symbol, bid_price, bid_qty, ask_price, ask_qty
                )
                print(f"✅ {symbol}: bid={bid_price} ask={ask_price}")

            except Exception as exc:
                logging.exception("Level 2 error for %s: %s", symbol, exc)
                print(f"❌ {symbol}: {exc}")


def is_market_open(now_dt: datetime) -> bool:
    return now_dt.weekday() < 5 and MARKET_OPEN <= now_dt.time() <= MARKET_CLOSE


def run_service():
    print("🐳 Level 2 order book service started")
    logging.info("Level 2 service started")
    driver = get_driver()

    try:
        while True:
            now_dt = datetime.now()
            if is_market_open(now_dt):
                scrape_iteration(driver)
            else:
                logging.info("Level 2 idle - market closed")
                print("😴 Market closed")

            time.sleep(CHECK_EVERY_SECONDS)

    except KeyboardInterrupt:
        logging.info("Level 2 stopped by user")
        print("🛑 Level 2 stopped")
    finally:
        driver.quit()


def run_once():
    driver = get_driver()
    try:
        scrape_iteration(driver)
    finally:
        driver.quit()


if __name__ == "__main__":
    run_mode = os.getenv("RUN_MODE", "service").lower()
    if run_mode == "once":
        run_once()
    else:
        run_service()