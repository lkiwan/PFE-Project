import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:123456@localhost:5432/PFE')
new_symbols = ['AKDITAL', 'ALLIANCES', 'JET CONTRACTORS', 'SGTM', 'SODEP', 'TAQA', 'TGCC', 'ADDOUHA']

with engine.begin() as conn:
    for sym in new_symbols:
        conn.execute(text("INSERT INTO ref.instruments (exchange_id, symbol, name, instrument_type, is_active) VALUES (1, :sym, :sym, 'Equity', TRUE) ON CONFLICT DO NOTHING"), {"sym": sym})
print('Inserted missing symbols')
