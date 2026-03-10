import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# 1. Connexion l-DB
engine = create_engine("postgresql://postgres:123456@localhost:5432/PFE")

# 2. List dyal les actions li bghiti (Symbols dyal Casablanca f Yahoo Finance)
stocks = {
    "TQA.MA": "TAQA MOROCCO",
    "IAM.MA": "MAROC TELECOM",
    "ATW.MA": "ATTIJARIWAFA BANK",
    "BCP.MA": "BCP",
    "MASI": "MASI" # L-Indice 
}

def ingest_historical_data():
    for symbol, full_name in stocks.items():
        print(f"🚀 Scraping {full_name} ({symbol})...")
        
        # Jbed 10 snin dyal d-data
        df = yf.download(symbol, start="2015-01-01", end="2026-03-01")
        
        if not df.empty:
            # Nettoyage s-ri3
            df = df.reset_index()
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            df['valeur_name'] = full_name
            
            # Formatting bach i-ji m3a la base dyalk
            # T-akked mn s-miyat dyal les colonnes f la base dyalk (cours_close, variation_pct, etc.)
            df_to_sql = df[['date', 'valeur_name', 'close', 'volume']]
            df_to_sql.columns = ['trade_date', 'valeur_name', 'cours_close', 'volume_mad']
            
            # Push l-PostgreSQL
            df_to_sql.to_sql('eod_bars', engine, schema='md', if_exists='append', index=False)
            print(f"✅ {len(df_to_sql)} lignes t-zadou f md.eod_bars")

if __name__ == "__main__":
    ingest_historical_data()