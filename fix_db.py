from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:123456@localhost:5432/PFE')

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ref.instrument_aliases (
            alias_id SERIAL PRIMARY KEY,
            instrument_id INTEGER REFERENCES ref.instruments(instrument_id),
            source_name VARCHAR(100),
            alias_value VARCHAR(100),
            UNIQUE(source_name, alias_value)
        );
    """))

print("Table ref.instrument_aliases created successfully.")
