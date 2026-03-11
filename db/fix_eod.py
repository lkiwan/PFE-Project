from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:123456@localhost:5432/PFE')

with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS md.eod_bars CASCADE;"))
    conn.execute(text("""
        CREATE TABLE md.eod_bars (
            id SERIAL PRIMARY KEY,
            instrument_id INTEGER REFERENCES ref.instruments(instrument_id),
            trade_date DATE,
            price NUMERIC,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            volume NUMERIC,
            change_pct NUMERIC,
            source_name VARCHAR(100),
            scraped_at TIMESTAMP,
            UNIQUE(instrument_id, trade_date)
        );
    """))

print("Table md.eod_bars recreated successfully with new schema.")
