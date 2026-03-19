"""
Technical indicators tool for AI Agent 1.
Computes SMA(5), SMA(10), RSI(14), and simple MACD.
"""
import json

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def compute_technical(symbol: str, max_date: str = None) -> str:
    """Compute technical indicators: SMA(5), SMA(10), RSI(14), and MACD for a stock.
    Returns the latest indicator values based on recent price data."""
    
    date_filter = "AND e.trade_date <= :max_date" if max_date else ""
    query = text(f"""
        SELECT e.trade_date, e.price AS close
        FROM md.eod_bars e
        JOIN ref.instruments i ON e.instrument_id = i.instrument_id
        WHERE i.symbol = :sym {date_filter}
        ORDER BY e.trade_date ASC
    """)
    with engine.connect() as conn:
        params = {"sym": symbol}
        if max_date:
            params["max_date"] = max_date
        df = pd.read_sql(query, conn, params=params)

    if df.empty or len(df) < 5:
        return json.dumps({"error": f"Not enough data for technical analysis on {symbol} (need ≥5 days, have {len(df)})"})

    # SMA (Simple Moving Average)
    df["sma_5"] = df["close"].rolling(5).mean()
    df["sma_10"] = df["close"].rolling(10).mean()

    # RSI (Relative Strength Index, 14-period)
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))

    # MACD (12-period EMA - 26-period EMA, signal = 9-period EMA of MACD)
    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_histogram"] = df["macd"] - df["macd_signal"]

    # Get latest values
    latest = df.iloc[-1]
    result = {
        "symbol": symbol,
        "trade_date": str(latest["trade_date"]),
        "close": float(latest["close"]),
        "sma_5": round(float(latest["sma_5"]), 2) if pd.notna(latest["sma_5"]) else None,
        "sma_10": round(float(latest["sma_10"]), 2) if pd.notna(latest["sma_10"]) else None,
        "rsi": round(float(latest["rsi"]), 2) if pd.notna(latest["rsi"]) else None,
        "macd": round(float(latest["macd"]), 2) if pd.notna(latest["macd"]) else None,
        "macd_signal": round(float(latest["macd_signal"]), 2) if pd.notna(latest["macd_signal"]) else None,
        "macd_histogram": round(float(latest["macd_histogram"]), 2) if pd.notna(latest["macd_histogram"]) else None,
        "sma_5_vs_sma_10": "BULLISH" if pd.notna(latest["sma_5"]) and pd.notna(latest["sma_10"]) and latest["sma_5"] > latest["sma_10"] else "BEARISH",
        "rsi_signal": (
            "OVERSOLD" if pd.notna(latest["rsi"]) and latest["rsi"] < 30
            else "OVERBOUGHT" if pd.notna(latest["rsi"]) and latest["rsi"] > 70
            else "NEUTRAL"
        ),
    }
    return json.dumps(result)
