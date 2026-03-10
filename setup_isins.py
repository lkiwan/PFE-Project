from sqlalchemy import create_engine, text

# Connexion l-PostgreSQL
DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL)

# L-Lista dyal l-ISINs li 3ndna daba (Top 10)
ISIN_MAPPING = {
    "IAM": "MA0000011488", "ATW": "MA0000010969", "LES": "MA0000011116",
    "ADH": "MA0000011512", "EQD": "MA0000010779", "SLF": "MA0000011215",
    "BCP": "MA0000011884", "BOA": "MA0000011504", "CIH": "MA0000011454",
    "LMC": "MA0000011801"
}

def setup_database():
    print("⚙️ Tajhiz dyal la base de données...")
    
    with engine.begin() as conn:
        # 1. Zid l-colonne 'isin' ila ma-kantch
        conn.execute(text("ALTER TABLE ref.instruments ADD COLUMN IF NOT EXISTS isin VARCHAR(50);"))
        print("✅ L-colonne 'isin' t-zadat (awla déjà kayna).")
        
        # 2. Insérer awla Update les charikat li f l-lista
        for symbol, isin in ISIN_MAPPING.items():
            # N-choufou wach l-symbol kayn
            result = conn.execute(text("SELECT instrument_id FROM ref.instruments WHERE symbol = :sym"), {"sym": symbol}).scalar()
            
            if result:
                # Ila kan kayn, ghir n-ziydou lih l-ISIN
                conn.execute(text("UPDATE ref.instruments SET isin = :isin WHERE symbol = :sym"), {"isin": isin, "sym": symbol})
            else:
                # Ila ma-kanch ga3, n-khelqouh mn jdid
                conn.execute(text("INSERT INTO ref.instruments (symbol, isin) VALUES (:sym, :isin)"), {"sym": symbol, "isin": isin})
                
        print(f"✅ {len(ISIN_MAPPING)} charikat t-gaddou b l-ISIN dyalhom f PostgreSQL.")

if __name__ == "__main__":
    setup_database()