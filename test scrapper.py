import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from datetime import datetime

engine = create_engine("postgresql://postgres:123456@localhost:5432/PFE")
TARGETS = ["EQD", "LES", "SLF", "IAM", "ATW", "ADH", "MASI"]

def debug_scrape():
    url = "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    print("🚀 Bdina n-qellbou f l-page...")
    res = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(res.content, 'html.parser')
    
# ... (dakchi li uwal dyal requests)
    tables = soup.find_all('table')
    print(f"📊 L9ina {len(tables)} tables. Kan-7elllou fihom...")

    with engine.begin() as conn:
        for table in tables:
            for tr in table.find_all('tr'):
                cols = tr.find_all('td')
                if len(cols) >= 5:
                    # N-jerrbou n-jbdou s-symbol (Symbol dima kiykoun f l-awal)
                    symbol = cols[0].get_text(strip=True).upper()
                    
                    if symbol in TARGETS:
                        # Parsing dyal l-prix (dima f l-column 4 aw 5)
                        try:
                            # Kan-7iyedo ayy 7aja machi rqem
                            price_raw = cols[4].get_text(strip=True).replace('\xa0', '').replace(' ', '').replace(',', '.')
                            price = float(price_raw)
                            
                            # 1. Check/Create Instrument
                            inst_id = conn.execute(text("SELECT instrument_id FROM ref.instruments WHERE symbol = :s"), {"s": symbol}).scalar()
                            if not inst_id:
                                inst_id = conn.execute(text("INSERT INTO ref.instruments (exchange_id, symbol, instrument_type, currency) VALUES (1, :s, 'EQUITY', 'MAD') RETURNING instrument_id"), {"s": symbol}).scalar()
                            
                            # 2. Upsert l-data b date s-7i7 (2026)
                            conn.execute(text("""
                                INSERT INTO md.eod_bars (instrument_id, bar_date, close)
                                VALUES (:id, CURRENT_DATE, :cl)
                                ON CONFLICT (instrument_id, bar_date) DO UPDATE SET close = EXCLUDED.close
                            """), {"id": inst_id, "cl": price})
                            
                            print(f"✅ Dkhlat: {symbol} -> {price} MAD")
                        except Exception as e:
                            continue

if __name__ == "__main__":
    debug_scrape()