from db.database import init_db
from scrapers.market_scraper import MarketDataScraper
from scrapers.financial_scraper import FinancialDataScraper
from scrapers.macro_scraper import MacroDataScraper
from engine.scoring_engine import run_scoring

def main():
    print("--- 1. Initialiser la Base de Données ---")
    init_db()
    
    print("\n--- 2. Exécuter l'Extraction des Données (Scraping) ---")
    market_scraper = MarketDataScraper()
    market_scraper.run()
    
    financial_scraper = FinancialDataScraper()
    financial_scraper.run()
    
    macro_scraper = MacroDataScraper()
    macro_scraper.run()
    
    print("\n--- 3. Exécuter le Moteur de Calcul (Scoring) ---")
    run_scoring()
    print("Pipeline terminé avec succès.")

if __name__ == "__main__":
    main()
