import os
import time
import logging
import certifi
from datetime import datetime

from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
SOURCE_NAME = "boursenews"
BASE_URL = "https://boursenews.ma/articles/marches"

TARGET_KEYWORDS = {
    # target markets
    "AKDITAL": ["akdital"],
    "ALLIANCES": ["alliances", "adi"],
    "ATW": ["attijariwafa", "atw"],
    "CIH": ["cih"],
    "ADDOHA-P": ["addoha", "douja"],
    "IAM": ["maroc telecom", "iam", "itissalat"],
    "JET CONTRACTORS": ["jet contractors", "jet"],
    "SGTM": ["sgtm"],
    "SODEP": ["marsa maroc", "sodep"],
    "TAQA": ["taqa"],
    "TGCC": ["tgcc"],
    
    # moroccan indices (mapped to None id)
    "MASI": ["masi", "masi20", "bourse de casablanca"]
}

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

logging.basicConfig(
    filename="boursenews_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123456@localhost:5432/PFE")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# -----------------------------------------------------------------------------
# WEBDRIVER INIT
# -----------------------------------------------------------------------------
def get_chrome_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

# -----------------------------------------------------------------------------
# DATABASE LOGIC
# -----------------------------------------------------------------------------
def get_instrument_mapping(conn):
    """Fetch mapping of symbols -> instrument_id from ref.instruments"""
    rows = conn.execute(
        text("SELECT instrument_id, symbol FROM ref.instruments WHERE is_active = TRUE")
    ).mappings().all()
    return {r["symbol"]: r["instrument_id"] for r in rows}

def save_articles_to_db(conn, articles):
    """
    Upserts the validated articles into md.news_articles.
    Ignores duplicates using the `link` column.
    """
    if not articles:
        return 0
        
    inserted = 0
    for article in articles:
        try:
            conn.execute(
                text("""
                    INSERT INTO md.news_articles (title, link, published_date, source_name, instrument_id)
                    VALUES (:title, :link, :published_date, :source_name, :instrument_id)
                    ON CONFLICT (link) DO NOTHING
                """),
                article
            )
            inserted += 1
        except Exception as e:
            logging.error(f"Failed to insert article '{article['title']}': {e}")
            
    return inserted

# -----------------------------------------------------------------------------
# SCRAPING LOGIC
# -----------------------------------------------------------------------------
def parse_article_block(block, instrument_map):
    """
    Extracts data from a single article block and filters by TARGET_KEYWORDS
    """
    try:
        a_tag = block.find_element(By.TAG_NAME, "a")
        link = a_tag.get_attribute("href")
        
        # In Boursenews structure, the headline and date text are usually inside the block
        text_content = block.text.strip()
        lines = text_content.split('\n')
        
        if not lines or len(lines) < 2:
            return None
            
        title = lines[0].strip()
        date_str = lines[-1].strip()  # Usually "Mardi 10 Mars 2026 - par ..."
        
        # Test against our keyword filter
        lower_title = title.lower()
        matched_symbol = None
        
        for root_sym, keywords in TARGET_KEYWORDS.items():
            if any(kw in lower_title for kw in keywords):
                matched_symbol = root_sym
                break
                
        if not matched_symbol:
            return None # Skip articles we don't care about
            
        # Get instrument_id (will be None for MASI / indices)
        inst_id = instrument_map.get(matched_symbol) if matched_symbol != "MASI" else None
        
        return {
            "title": title,
            "link": link,
            "published_date": date_str,
            "source_name": SOURCE_NAME,
            "instrument_id": inst_id,
            "symbol": matched_symbol # for printing
        }
    except Exception as e:
        # Some blocks might be malformed ads or empty spaces
        return None

def scrape_boursenews():
    driver = None
    try:
        print(f"🚀 Initializing BourseNews WebDriver...")
        driver = get_chrome_driver()
        driver.get(BASE_URL)
        
        # Wait for article grid to load (divs containing 'a' tags)
        print("   [+] Waiting for page to render...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/article/']"))
        )
        
        # Get all parent elements holding articles. On Boursenews, titles are h4/h3/h2 or inside flex rows.
        # Grabbing all anchor tags pointing to /article/ is the safest bet to find the cards.
        article_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/article/']")
        # We need their parent containers to get the text + date properly
        blocks = [a.find_element(By.XPATH, "..") for a in article_links]
        
        # Deduplicate blocks since an image and title might wrap in two <a> tags in the same block
        unique_blocks = {b.id: b for b in blocks}.values()
        
        print(f"   [+] Found {len(unique_blocks)} article blocks. Reconciling with database...")
        
        with engine.begin() as conn:
            inst_map = get_instrument_mapping(conn)
            
            valid_articles = []
            for block in unique_blocks:
                parsed = parse_article_block(block, inst_map)
                if parsed:
                    valid_articles.append(parsed)
                    
            if not valid_articles:
                print("   [!] No articles matched your target stocks or MASI indicator.")
                return
                
            print(f"   [+] Filtered down to {len(valid_articles)} relevant articles.")
            for a in valid_articles:
                print(f"       ✅ [{a['symbol']}] {a['title']}")
                
            saved = save_articles_to_db(conn, valid_articles)
            print(f"   [+] Inserted {saved} new articles into md.news_articles!")

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        logging.error(f"Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    scrape_boursenews()
