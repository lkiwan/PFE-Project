import os
import certifi
import time
import json
import logging # <--- Jdid
from datetime import datetime, time as dt_time
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# 1. Configuration dyal l-Logging
logging.basicConfig(
    filename='trading_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- CONFIGURATION & SSL FIX ---
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL)

ISIN_MAPPING = {
    "IAM": "MA0000011488", "ATW": "MA0000010969", "LES": "MA0000011116",
    "ADH": "MA0000011512", "EQD": "MA0000010779", "SLF": "MA0000011215"
}

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def scrape_iteration(driver):
    logging.info("🚀 Iteration bdlat.") # Sejel f l-fichier
    
    with engine.begin() as conn:
        for symbol, isin in ISIN_MAPPING.items():
            try:
                api_url = f"https://medias24.com/content/api?method=getBidAsk&ISIN={isin}&format=json"
                driver.get(api_url)
                time.sleep(2) 
                
                raw_data = driver.find_element("tag name", "body").text
                data = json.loads(raw_data)
                
                if data.get("result") and data["result"].get("orderBook"):
                    best = data["result"]["orderBook"][0]
                    inst_id = conn.execute(text("SELECT instrument_id FROM ref.instruments WHERE symbol = :s"), {"s": symbol}).scalar()
                    
                    if inst_id:
                        conn.execute(text("""
                            INSERT INTO md.order_books (instrument_id, bid_price, bid_qty, ask_price, ask_qty)
                            VALUES (:id, :bp, :bq, :ap, :aq)
                        """), {
                            "id": inst_id, "bp": best.get("bidValue"), "bq": best.get("bidQte"), 
                            "ap": best.get("askValue"), "aq": best.get("askQte")
                        })
                        logging.info(f"✅ {symbol}: Bid={best.get('bidValue')} | Ask={best.get('askValue')}")
                else:
                    logging.warning(f"🛑 {symbol}: Carnet khawi.") # Sejel warning

            except Exception as e:
                logging.error(f"❌ Error f {symbol}: {e}") # Sejel l-erreur
                continue

def main():
    logging.info("🐳 Whale Scraper started.")
    print("🐳 Whale Scraper is starting (Check 'trading_bot.log' for details)...")
    driver = get_driver()
    
    try:
        while True:
            now_time = datetime.now().time()
            # Market Hours: 09:30 - 15:40
            if datetime.now().weekday() < 5 and dt_time(9, 30) <= now_time <= dt_time(15, 40):
                scrape_iteration(driver)
            else:
                logging.info("😴 Bourse sadda (Weekend aw f l-lil).")
            
            time.sleep(300) 
            
    except KeyboardInterrupt:
        logging.info("👋 Scraper t-7bess b yeddik.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()