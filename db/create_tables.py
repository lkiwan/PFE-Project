"""
Create the md.predictions and md.historical_prices tables.
Safe to re-run — uses IF NOT EXISTS.
"""
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


def create_tables():
    with engine.begin() as conn:
        # ── md.historical_prices ─────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS md.historical_prices (
                id              SERIAL PRIMARY KEY,
                symbol          VARCHAR(50) NOT NULL,
                timeframe       VARCHAR(10) NOT NULL,
                trade_date      DATE NOT NULL,
                close_price     FLOAT NOT NULL,
                open_price      FLOAT,
                high            FLOAT,
                low             FLOAT,
                volume          FLOAT,
                change_pct      FLOAT,
                UNIQUE(symbol, timeframe, trade_date)
            );
        """))
        print("✅ md.historical_prices — ready")

        # ── md.predictions ───────────────────────────────────────────────
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS md.predictions (
                id               SERIAL PRIMARY KEY,
                instrument_id    INTEGER,
                symbol           VARCHAR(50) NOT NULL,
                analysis_date    DATE NOT NULL DEFAULT CURRENT_DATE,

                -- Technical Analysis
                trend            VARCHAR(20) NOT NULL,
                strength         INTEGER CHECK (strength BETWEEN 1 AND 10),
                support_price    FLOAT,
                resistance_price FLOAT,

                -- AI Prediction
                predicted_action VARCHAR(20) NOT NULL,
                confidence_pct   FLOAT CHECK (confidence_pct BETWEEN 0 AND 100),
                predicted_move   FLOAT,
                reasoning        TEXT NOT NULL,

                -- Context Used
                news_sentiment   VARCHAR(20),
                masi_trend       VARCHAR(20),
                orderbook_bias   VARCHAR(20),

                -- Metadata
                model_name       VARCHAR(100) DEFAULT 'qwen2.5:7b',
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(symbol, analysis_date)
            );
        """))
        print("✅ md.predictions — ready")


if __name__ == "__main__":
    create_tables()
    print("\n🎉 All tables created successfully!")
