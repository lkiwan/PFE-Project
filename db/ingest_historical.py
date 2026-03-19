"""
One-time script to load all historical CSVs from data/historical data/
into md.historical_prices.

Auto-detects English vs French CSV format and normalizes all data.
Safe to re-run — uses ON CONFLICT DO NOTHING.
"""
import os
import re

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# Map folder names → DB symbols (matching ref.instruments)
FOLDER_TO_SYMBOL = {
    "AKDITAL": "AKDITAL",
    "ALLIANCES": "ALLIANCES",
    "ATW": "ATW",
    "cih": "CIH",
    "DOUJA PROM ADDOHA": "ADDOUHA",
    "IAM": "IAM",
    "JET CONTRACTORS": "JET CONTRACTORS",
    "MASI": "MASI",
    "MASI 20": "MASI 20",
    "SGTM S.A": "SGTM",
    "SODEP-Marsa Maroc": "SODEP",
    "TAQA MOROCCO": "TAQA",
    "TGCC": "TGCC",
}


def detect_timeframe(filename: str) -> str:
    """Detect timeframe from filename."""
    fl = filename.lower()
    if "daily" in fl or "dayli" in fl:
        return "daily"
    elif "weekly" in fl:
        return "weekly"
    elif "monthly" in fl:
        return "monthly"
    return "daily"


def detect_format(columns: list) -> str:
    """Detect English vs French CSV by checking column names."""
    joined = " ".join(str(c).lower() for c in columns)
    if "dernier" in joined or "ouv." in joined or "variation" in joined:
        return "french"
    return "english"


def parse_volume(val) -> float | None:
    """Parse volume strings like '37.32K', '1.23M', '121,73K', '-'."""
    if val is None or pd.isna(val):
        return None
    s = str(val).strip().replace('"', '')
    if not s or s == "-":
        return None

    multiplier = 1
    if s.upper().endswith("K"):
        multiplier = 1_000
        s = s[:-1]
    elif s.upper().endswith("M"):
        multiplier = 1_000_000
        s = s[:-1]
    elif s.upper().endswith("B"):
        multiplier = 1_000_000_000
        s = s[:-1]

    # Handle European format: comma as decimal, dot as thousands
    if "," in s and "." in s:
        # Could be "37.32" (English thousands) or "1.234,56" (European)
        # Check which comes last — if comma is last, it's European decimal
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            # European: "1.234,56" → remove dots, replace comma with dot
            s = s.replace(".", "").replace(",", ".")
        else:
            # English: "1,234.56" → remove commas
            s = s.replace(",", "")
    elif "," in s:
        # Only comma — could be European decimal "121,73"
        s = s.replace(",", ".")
    else:
        # Only dots or no separator — remove any thousands commas
        s = s.replace(",", "")

    try:
        return float(s) * multiplier
    except ValueError:
        return None


def parse_change_pct(val) -> float | None:
    """Parse change % like '-2.29%' or '-0,97%'."""
    if val is None or pd.isna(val):
        return None
    s = str(val).strip().replace('"', '').replace('%', '').strip()
    if not s or s == "-":
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_price_english(val) -> float | None:
    """Parse English price like '1,065.00' (comma = thousands)."""
    if val is None or pd.isna(val):
        return None
    s = str(val).strip().replace('"', '')
    if not s or s == "-":
        return None
    s = s.replace(",", "")  # remove thousands separator
    try:
        return float(s)
    except ValueError:
        return None


def parse_price_french(val) -> float | None:
    """Parse French price like '701,10' (comma = decimal)."""
    if val is None or pd.isna(val):
        return None
    s = str(val).strip().replace('"', '')
    if not s or s == "-":
        return None
    # European: dots are thousands, comma is decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def process_csv(filepath: str, symbol: str, timeframe: str) -> pd.DataFrame:
    """Read a single CSV file and return normalized DataFrame."""
    df = pd.read_csv(filepath, encoding="utf-8")
    fmt = detect_format(df.columns.tolist())

    rows = []
    for _, row in df.iterrows():
        if fmt == "english":
            # English: Date=MM/DD/YYYY, Price, Open, High, Low, Vol., Change %
            try:
                trade_date = pd.to_datetime(row.iloc[0], format="%m/%d/%Y").date()
            except Exception:
                continue
            parse_fn = parse_price_english
            close = parse_fn(row.iloc[1])
            open_p = parse_fn(row.iloc[2])
            high = parse_fn(row.iloc[3])
            low = parse_fn(row.iloc[4])
        else:
            # French: Date=DD/MM/YYYY, Dernier, Ouv., Plus Haut, Plus Bas, Vol., Variation %
            try:
                trade_date = pd.to_datetime(row.iloc[0], format="%d/%m/%Y").date()
            except Exception:
                continue
            parse_fn = parse_price_french
            close = parse_fn(row.iloc[1])
            open_p = parse_fn(row.iloc[2])
            high = parse_fn(row.iloc[3])
            low = parse_fn(row.iloc[4])

        vol = parse_volume(row.iloc[5]) if len(row) > 5 else None
        chg = parse_change_pct(row.iloc[6]) if len(row) > 6 else None

        if close is None:
            continue

        rows.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "trade_date": trade_date,
            "close_price": close,
            "open_price": open_p,
            "high": high,
            "low": low,
            "volume": vol,
            "change_pct": chg,
        })

    return pd.DataFrame(rows)


def ingest_all():
    base_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "historical data")

    if not os.path.exists(base_dir):
        print(f"❌ Directory not found: {base_dir}")
        return

    total_rows = 0

    for folder_name in sorted(os.listdir(base_dir)):
        folder_path = os.path.join(base_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue

        symbol = FOLDER_TO_SYMBOL.get(folder_name)
        if symbol is None:
            print(f"⚠️  Skipping unknown folder: {folder_name}")
            continue

        for csv_file in sorted(os.listdir(folder_path)):
            if not csv_file.lower().endswith(".csv"):
                continue

            filepath = os.path.join(folder_path, csv_file)
            timeframe = detect_timeframe(csv_file)

            try:
                df = process_csv(filepath, symbol, timeframe)
            except Exception as e:
                print(f"   ❌ Error processing {csv_file}: {e}")
                continue

            if df.empty:
                print(f"   ⚠️  No valid rows: {csv_file}")
                continue

            # Bulk upsert
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(
                        text("""
                            INSERT INTO md.historical_prices
                                (symbol, timeframe, trade_date, close_price, open_price, high, low, volume, change_pct)
                            VALUES
                                (:symbol, :timeframe, :trade_date, :close_price, :open_price, :high, :low, :volume, :change_pct)
                            ON CONFLICT (symbol, timeframe, trade_date) DO UPDATE SET
                                close_price = EXCLUDED.close_price,
                                open_price  = EXCLUDED.open_price,
                                high        = EXCLUDED.high,
                                low         = EXCLUDED.low,
                                volume      = EXCLUDED.volume,
                                change_pct  = EXCLUDED.change_pct
                        """),
                        row.to_dict(),
                    )

            total_rows += len(df)
            print(f"   ✅ {symbol}/{timeframe}: {len(df)} rows from {csv_file}")

    print(f"\n🎉 Total ingested: {total_rows} rows into md.historical_prices")


if __name__ == "__main__":
    ingest_all()
