import os
import certifi
from datetime import datetime
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()

DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"
engine = create_engine(DB_URL)

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    # 🛑 HADA HOUWA L-7ELL L-KBIR: Forci l-écran bach i-ban b7al PC machi téléphone
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def clean_number(text_val):
    if not text_val: return None
    cleaned = text_val.replace('\xa0', '').replace(' ', '').replace('%', '').replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return None

def scrape_indices():
    url = "https://www.casablanca-bourse.com/fr/live-market/marche-cash/indices"
    print("📊 Extraction COMPLÈTE des indices via Selenium...")
    driver = get_driver()
    
    target_indices = ["MASI", "MASI 20", "MASI ESG", "MASI Mid and Small Cap"]
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 15)
        
        # Tsna 7ta i-ban l-tableau f l-page
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS md.market_indices (
                    id SERIAL PRIMARY KEY,
                    index_name VARCHAR(50),
                    valeur NUMERIC,
                    veille NUMERIC,
                    variation_pct NUMERIC,
                    variation_ytd NUMERIC,
                    plus_haut NUMERIC,
                    plus_bas NUMERIC,
                    record_date DATE DEFAULT CURRENT_DATE,
                    UNIQUE(index_name, record_date)
                )
            """))
            
            # 🛑 MANTI9 JDID: Jbed ga3 s-toura (rows) dyal l-tableau
            rows = driver.find_elements(By.XPATH, "//table//tr")
            found_count = 0
            
            for row in rows:
                columns = row.find_elements(By.TAG_NAME, "td")
                
                # Ila kan s-ter fih 7 dyal l-khanat kima f t-swira
                if len(columns) >= 7:
                    index_name = columns[0].text.strip()
                    
                    # Wach had l-index mn dok li bghina?
                    if index_name in target_indices:
                        valeur = clean_number(columns[1].text)
                        veille = clean_number(columns[2].text)
                        var_pct = clean_number(columns[3].text)
                        var_ytd = clean_number(columns[4].text)
                        plus_haut = clean_number(columns[5].text)
                        plus_bas = clean_number(columns[6].text)
                        
                        conn.execute(text("""
                            INSERT INTO md.market_indices (
                                index_name, valeur, veille, variation_pct, variation_ytd, plus_haut, plus_bas, record_date
                            ) VALUES (
                                :name, :val, :veille, :var_pct, :var_ytd, :ph, :pb, CURRENT_DATE
                            )
                            ON CONFLICT (index_name, record_date) 
                            DO UPDATE SET 
                                valeur = EXCLUDED.valeur,
                                veille = EXCLUDED.veille,
                                variation_pct = EXCLUDED.variation_pct,
                                variation_ytd = EXCLUDED.variation_ytd,
                                plus_haut = EXCLUDED.plus_haut,
                                plus_bas = EXCLUDED.plus_bas
                        """), {
                            "name": index_name, "val": valeur, "veille": veille, 
                            "var_pct": var_pct, "var_ytd": var_ytd, "ph": plus_haut, "pb": plus_bas
                        })
                        
                        print(f"✅ {index_name} enregistré: Valeur={valeur} | Var={var_pct}% | PlusHaut={plus_haut}")
                        found_count += 1
            
            if found_count == 0:
                print("⚠️ L-bot lqa l-tableau, walakin ma-lqach s-miyat dyal les indices. L-site ghadi i-koun m-beddel l-HTML dyalo.")
                
        print(f"📅 Mise à jour terminée ({datetime.now().date()})")
        
    except Exception as e:
        # Daba ila tra chi mouchkil, ghadi n-choufouh b-dabt
        print(f"❌ Erreur globale Selenium : {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_indices()