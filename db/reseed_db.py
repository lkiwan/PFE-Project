import os
from sqlalchemy import create_engine, text

db_url = 'postgresql://postgres:123456@localhost:5432/PFE'
engine = create_engine(db_url)

with engine.begin() as conn:
    print("Re-seeding Exchange...")
    conn.execute(text("INSERT INTO ref.exchanges (exchange_id, code, name) VALUES (1, 'CSE', 'Casablanca Stock Exchange') ON CONFLICT DO NOTHING"))
    
    print("Re-seeding Instruments...")
    symbols = ['AKDITAL', 'ALLIANCES', 'ATW', 'CIH', 'ADDOHA-P', 'IAM', 'JET CONTRACTORS', 'SGTM', 'SODEP', 'TAQA', 'TGCC']
    added = 0
    for s in symbols:
        res = conn.execute(text("INSERT INTO ref.instruments (exchange_id, symbol, name, instrument_type, is_active) VALUES (1, :sym, :sym, 'Equity', TRUE) ON CONFLICT DO NOTHING"), {'sym': s})
        added += res.rowcount
    
    print(f"Successfully re-seeded {added} symbols!")
