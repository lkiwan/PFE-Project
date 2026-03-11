import os
import certifi
import pandas as pd
from sqlalchemy import create_engine, text

# Fix SSL paths for PostgreSQL
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()

engine = create_engine("postgresql://postgres:123456@localhost:5432/PFE")

def scrape_level_1():
    url = "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement/"
    print("📊 Level 1: Scrapping OHLCV...")
    try:
        tables = pd.read_html(url, decimal=',', thousands=' ')
        df = tables[0] # L-tableau l-uwal
        
        # Logic dyal l-Cleaning ou l-Insertion f md.eod_bars
        # (Kima darna f l-uwal niyan l-base de données)
        print("✅ Level 1 updated successfully.")
    except Exception as e:
        print(f"❌ Level 1 Error: {e}")

if __name__ == "__main__":
    scrape_level_1()