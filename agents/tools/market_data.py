"""
Market data tools for AI Agent 1.
- get_stock_history:    last 18 days from md.eod_bars
- get_historical_trend: long-term data from md.historical_prices
- get_masi_index:       MASI index from md.market_index
- get_orderbook:        bid/ask snapshots from md.order_books
"""
import json

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def get_stock_history(symbol: str, max_date: str = None) -> str:
    """Fetch the last 18 days of daily price data (OHLCV) for a stock.
    Use this for short-term trend analysis and recent price movements."""
    
    date_filter = "AND e.trade_date <= :max_date" if max_date else ""
    query = text(f"""
        SELECT e.trade_date, e.price AS close, e.open, e.high, e.low,
               e.volume, e.change_pct
        FROM md.eod_bars e
        JOIN ref.instruments i ON e.instrument_id = i.instrument_id
        WHERE i.symbol = :sym {date_filter}
        ORDER BY e.trade_date DESC
        LIMIT 18
    """)
    with engine.connect() as conn:
        params = {"sym": symbol}
        if max_date:
            params["max_date"] = max_date
        df = pd.read_sql(query, conn, params=params)
    if df.empty:
        return json.dumps({"error": f"No EOD data found for {symbol}"})
    # Convert dates to string for JSON serialization
    df["trade_date"] = df["trade_date"].astype(str)
    return df.to_json(orient="records")


def get_historical_trend(symbol: str, timeframe: str = "weekly", max_date: str = None) -> str:
    """Fetch long-term price history (weekly or monthly) for deeper trend analysis.
    Use this for understanding multi-month/year patterns, 52-week highs/lows,
    and seasonal trends. Timeframe must be 'weekly' or 'monthly'."""
    
    date_filter = "AND trade_date <= :max_date" if max_date else ""
    query = text(f"""
        SELECT trade_date, close_price, change_pct
        FROM md.historical_prices
        WHERE symbol = :sym AND timeframe = :tf {date_filter}
        ORDER BY trade_date DESC
        LIMIT 52
    """)
    with engine.connect() as conn:
        params = {"sym": symbol, "tf": timeframe}
        if max_date:
            params["max_date"] = max_date
        df = pd.read_sql(query, conn, params=params)
    if df.empty:
        return json.dumps({"error": f"No historical data for {symbol} ({timeframe})"})

    stats = {
        "symbol": symbol,
        "timeframe": timeframe,
        "data_points": len(df),
        "52w_high": float(df["close_price"].max()),
        "52w_low": float(df["close_price"].min()),
        "avg_price": round(float(df["close_price"].mean()), 2),
        "current_vs_52w_high_pct": round(
            (float(df["close_price"].iloc[0]) / float(df["close_price"].max()) - 1) * 100, 2
        ),
        "recent_prices": json.loads(
            df.head(10).assign(trade_date=df["trade_date"].astype(str)).to_json(orient="records")
        ),
    }
    return json.dumps(stats)


def get_masi_index(max_date: str = None) -> str:
    """Fetch the MASI index history (last 18 days) for overall market context.
    Use this to understand the general market direction."""
    
    date_filter = "AND trade_date <= :max_date" if max_date else ""
    query = text(f"""
        SELECT trade_date, close_price, change_pct
        FROM md.market_index
        WHERE index_name = 'MASI' {date_filter}
        ORDER BY trade_date DESC
        LIMIT 18
    """)
    with engine.connect() as conn:
        params = {}
        if max_date:
            params["max_date"] = max_date
        df = pd.read_sql(query, conn, params=params)
    if df.empty:
        return json.dumps({"error": "No MASI index data found"})
    df["trade_date"] = df["trade_date"].astype(str)
    return df.to_json(orient="records")


def get_orderbook(symbol: str, max_date: str = None) -> str:
    """Fetch the latest 5 order book snapshots (bid/ask prices and quantities).
    Use this to assess buy/sell pressure for the stock."""
    
    date_filter = "AND DATE(ob.snapshot_time) <= :max_date" if max_date else ""
    query = text(f"""
        SELECT ob.bid_price, ob.bid_qty, ob.ask_price, ob.ask_qty,
               ob.snapshot_time
        FROM md.order_books ob
        JOIN ref.instruments i ON ob.instrument_id = i.instrument_id
        WHERE i.symbol = :sym {date_filter}
        ORDER BY ob.snapshot_time DESC
        LIMIT 5
    """)
    with engine.connect() as conn:
        params = {"sym": symbol}
        if max_date:
            params["max_date"] = max_date
        df = pd.read_sql(query, conn, params=params)
    if df.empty:
        return json.dumps({"error": f"No orderbook data for {symbol}"})
    df["snapshot_time"] = df["snapshot_time"].astype(str)
    return df.to_json(orient="records")
