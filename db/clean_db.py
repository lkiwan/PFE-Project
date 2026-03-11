from sqlalchemy import create_engine, text

db_url = 'postgresql://postgres:123456@localhost:5432/PFE'
engine = create_engine(db_url)

target_ids = (1, 4, 9, 11, 12, 13, 14, 15, 16, 17, 19)

with engine.begin() as conn:
    print("Deleting target instrument_ids from order_books...")
    r1 = conn.execute(text(f"DELETE FROM md.order_books WHERE instrument_id IN {target_ids}"))
    print("Deleted rows from order_books:", r1.rowcount)
    
    print("Deleting from eod_bars...")
    r2 = conn.execute(text(f"DELETE FROM md.eod_bars WHERE instrument_id IN {target_ids}"))
    print("Deleted rows from eod_bars:", r2.rowcount)
