from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:123456@localhost:5432/PFE')

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS md.market_index (
            id SERIAL PRIMARY KEY,
            index_name VARCHAR(50) NOT NULL,
            trade_date DATE NOT NULL,
            close_price FLOAT,
            open_price FLOAT,
            high FLOAT,
            low FLOAT,
            volume FLOAT,
            change_pct FLOAT,
            source_name VARCHAR(100) DEFAULT 'investing',
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(index_name, trade_date)
        );
    """))
    print("Table md.market_index created!")
