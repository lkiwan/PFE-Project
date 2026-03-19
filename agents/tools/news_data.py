"""
News data tool for AI Agent 1.
"""
import json

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def get_stock_news(symbol: str) -> str:
    """Fetch recent news articles mentioning this stock or MASI.
    Use this to assess news sentiment (positive/negative/neutral)."""
    query = text("""
        SELECT na.title, na.published_date, na.source_name
        FROM md.news_articles na
        WHERE na.instrument_id = (
            SELECT instrument_id FROM ref.instruments WHERE symbol = :sym
        )
        OR na.title ILIKE '%MASI%'
        ORDER BY na.scraped_at DESC
        LIMIT 10
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"sym": symbol})
    if df.empty:
        return json.dumps({"info": f"No recent news found for {symbol}"})
    df["published_date"] = df["published_date"].astype(str)
    return df.to_json(orient="records")
