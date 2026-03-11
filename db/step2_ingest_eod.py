import argparse
import hashlib
import json
import os
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_MAPPING = {
    # required
    "date": "Y",        # 'Y' hiya l-tarikh (Date)
    "symbol": "X1",     # 'X1' hiya l-action (Symbol)
    # optional market columns
    "open": "X2",       # 'X2' masalan hiya l-prix d'ouverture
    "high": None,
    "low": None,
    "close": "X3",      # 'X3' hiya l-prix de clôture (Darouriya)
    "adj_close": None,
    "volume": "X4",     # 'X4' hiya l-volume
    "turnover": None,
    "vwap": None,
    # optional instrument metadata
    "name": None,
    "currency": None,
}


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_mapping(mapping_path: Optional[str]) -> Dict[str, Optional[str]]:
    if not mapping_path:
        return DEFAULT_MAPPING.copy()
    with open(mapping_path, "r", encoding="utf-8") as f:
        user_map = json.load(f)
    m = DEFAULT_MAPPING.copy()
    m.update(user_map)
    return m


def read_eod_file(path: str, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    ext = os.path.splitext(path.lower())[1]
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif ext in [".csv"]:
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}. Use CSV or XLSX.")

    # validate required fields exist
    for required_key in ["date", "symbol", "close"]:
        col = mapping.get(required_key)
        if not col or col not in df.columns:
            raise ValueError(
                f"Missing required column mapping for '{required_key}'. "
                f"Expected column '{col}' in file."
            )

    # normalize output dataframe to canonical columns
    out = pd.DataFrame()
    # Hna kan-goulou l-Pandas y-fhem l-format s-7i7 (masalan DD/MM/YYYY)
    out["bar_date"] = pd.to_datetime(df[mapping["date"]], dayfirst=True, errors='coerce').dt.date
    out["symbol"] = df[mapping["symbol"]].astype(str).str.strip()

    def num(col_key: str) -> pd.Series:
        col_name = mapping.get(col_key)
        if col_name and col_name in df.columns:
            return pd.to_numeric(df[col_name], errors="coerce")
        return pd.Series([None] * len(df))

    out["open"] = num("open")
    out["high"] = num("high")
    out["low"] = num("low")
    out["close"] = num("close")
    out["adj_close"] = num("adj_close")
    out["volume"] = num("volume")
    out["turnover"] = num("turnover")
    out["vwap"] = num("vwap")

    # optional instrument info
    name_col = mapping.get("name")
    cur_col = mapping.get("currency")
    out["name"] = df[name_col].astype(str).str.strip() if name_col and name_col in df.columns else None
    out["currency"] = df[cur_col].astype(str).str.strip() if cur_col and cur_col in df.columns else None

    # drop rows with missing essentials
    out = out.dropna(subset=["bar_date", "symbol", "close"])
    return out


def ensure_source(conn, source_name: str, base_url: Optional[str]) -> int:
    conn.execute(
        text("""
            INSERT INTO ingest.sources (name, base_url)
            VALUES (:name, :base_url)
            ON CONFLICT (name) DO UPDATE SET base_url = COALESCE(EXCLUDED.base_url, ingest.sources.base_url)
        """),
        {"name": source_name, "base_url": base_url},
    )
    source_id = conn.execute(
        text("SELECT source_id FROM ingest.sources WHERE name = :name"),
        {"name": source_name},
    ).scalar_one()
    return int(source_id)


def register_raw_file(conn, source_id: int, url: Optional[str], content_hash: str, status: str, notes: str) -> str:
    file_id = conn.execute(
        text("""
            INSERT INTO ingest.raw_files (source_id, url, content_hash, status, notes)
            VALUES (:source_id, :url, :content_hash, :status, :notes)
            RETURNING file_id
        """),
        {
            "source_id": source_id,
            "url": url,
            "content_hash": content_hash,
            "status": status,
            "notes": notes[:5000],
        },
    ).scalar_one()
    return str(file_id)


def get_exchange_id(conn, code: str = "CSE") -> int:
    ex_id = conn.execute(
        text("SELECT exchange_id FROM ref.exchanges WHERE code = :code"),
        {"code": code},
    ).scalar_one_or_none()
    if ex_id is None:
        raise RuntimeError("Exchange CSE not found. Run Step 1 schema (and seed) in PFE DB.")
    return int(ex_id)


def get_or_create_instrument(
    conn,
    exchange_id: int,
    instrument_type: str,
    symbol: str,
    name: Optional[str] = None,
    currency: str = "MAD",
) -> int:
    symbol = symbol.strip()
    row = conn.execute(
        text("""
            SELECT instrument_id
            FROM ref.instruments
            WHERE exchange_id = :exchange_id AND symbol = :symbol
        """),
        {"exchange_id": exchange_id, "symbol": symbol},
    ).scalar_one_or_none()

    if row is not None:
        return int(row)

    new_id = conn.execute(
        text("""
            INSERT INTO ref.instruments (exchange_id, instrument_type, symbol, name, currency, is_active)
            VALUES (:exchange_id, :instrument_type, :symbol, :name, :currency, TRUE)
            RETURNING instrument_id
        """),
        {
            "exchange_id": exchange_id,
            "instrument_type": instrument_type,
            "symbol": symbol,
            "name": name,
            "currency": currency or "MAD",
        },
    ).scalar_one()
    return int(new_id)


def upsert_eod_bars(conn, df: pd.DataFrame, source_file_id: str, exchange_id: int, instrument_type: str):
    # Prepare rows with instrument_id resolved
    rows = []
    for r in df.itertuples(index=False):
        currency = r.currency if r.currency and str(r.currency).strip() else "MAD"
        inst_id = get_or_create_instrument(
            conn,
            exchange_id=exchange_id,
            instrument_type=instrument_type,
            symbol=r.symbol,
            name=r.name if r.name else None,
            currency=currency,
        )
        rows.append(
            {
                "instrument_id": inst_id,
                "bar_date": r.bar_date,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "adj_close": r.adj_close,
                "volume": r.volume,
                "turnover": r.turnover,
                "vwap": r.vwap,
                "source_file_id": source_file_id,
            }
        )

    if not rows:
        print("No valid rows to ingest.")
        return

    # Bulk upsert
    conn.execute(
        text("""
            INSERT INTO md.eod_bars (
              instrument_id, bar_date, open, high, low, close, adj_close, volume, turnover, vwap, source_file_id
            )
            VALUES (
              :instrument_id, :bar_date, :open, :high, :low, :close, :adj_close, :volume, :turnover, :vwap, :source_file_id
            )
            ON CONFLICT (instrument_id, bar_date) DO UPDATE SET
              open = EXCLUDED.open,
              high = EXCLUDED.high,
              low  = EXCLUDED.low,
              close = EXCLUDED.close,
              adj_close = COALESCE(EXCLUDED.adj_close, md.eod_bars.adj_close),
              volume = COALESCE(EXCLUDED.volume, md.eod_bars.volume),
              turnover = COALESCE(EXCLUDED.turnover, md.eod_bars.turnover),
              vwap = COALESCE(EXCLUDED.vwap, md.eod_bars.vwap),
              source_file_id = EXCLUDED.source_file_id
        """),
        rows,
    )
    print(f"✅ Upserted {len(rows)} rows into md.eod_bars")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=os.environ.get("DATABASE_URL"), help="Postgres URL (or set DATABASE_URL)")
    p.add_argument("--file", required=True, help="Path to CSV/XLSX export file")
    p.add_argument("--source-name", default="CSE_export", help="ingest.sources.name")
    p.add_argument("--base-url", default=None, help="Optional base URL for the source")
    p.add_argument("--url", default=None, help="Optional exact URL for this file (if downloaded)")
    p.add_argument("--mapping", default=None, help="JSON mapping file path (optional)")
    p.add_argument("--instrument-type", default="EQUITY", help="EQUITY or INDEX or RATE_PROXY")
    args = p.parse_args()

    if not args.db:
        raise SystemExit("Missing --db or DATABASE_URL env var")

    mapping = load_mapping(args.mapping)
    df = read_eod_file(args.file, mapping)

    file_hash = sha256_file(args.file)
    notes = f"Imported {os.path.basename(args.file)} rows={len(df)} at {datetime.utcnow().isoformat()}Z"

    engine = create_engine(args.db, pool_pre_ping=True, future=True)

    with engine.begin() as conn:
        exchange_id = get_exchange_id(conn, "CSE")
        source_id = ensure_source(conn, args.source_name, args.base_url)
        file_id = register_raw_file(conn, source_id, args.url, file_hash, "OK", notes)

        upsert_eod_bars(
            conn=conn,
            df=df,
            source_file_id=file_id,
            exchange_id=exchange_id,
            instrument_type=args.instrument_type,
        )


if __name__ == "__main__":
    main()