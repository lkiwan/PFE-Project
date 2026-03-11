"""
Medias24 Financial News Scraper
Scrapes all pages from https://medias24.com/categorie/leboursier/bourse-leboursier/page/N/
and stores relevant articles in md.news_articles (PostgreSQL).
"""
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
from bs4 import BeautifulSoup

# ----- SSL Fix -----
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

# ----- CONFIG -----
SOURCE_NAME = "medias24"
BASE_URL = "https://medias24.com/categorie/leboursier/bourse-leboursier/page/{page}/"
MAX_PAGES = 68  # discovered from pagination

# Keywords to match articles (case-insensitive search on titles)
TARGET_KEYWORDS = {
    "AKDITAL":          ["akdital"],
    "ALLIANCES":        ["alliances"],
    "ATW":              ["attijariwafa", "attijari"],
    "CIH":              ["cih"],
    "ADDOHA-P":         ["addoha", "douja"],
    "IAM":              ["maroc telecom", "iam", "itissalat"],
    "JET CONTRACTORS":  ["jet contractors", "jet contractor"],
    "SGTM":             ["sgtm"],
    "SODEP":            ["marsa maroc", "sodep"],
    "TAQA":             ["taqa"],
    "TGCC":             ["tgcc"],
    # Moroccan indicators (no instrument_id)
    "MASI":             ["masi", "msi20", "bourse de casablanca"],
}

logging.basicConfig(
    filename="medias24_news_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123456@localhost:5432/PFE")
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)


# ----- WEBDRIVER -----
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


# ----- DB HELPERS -----
def get_instrument_mapping():
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT instrument_id, symbol FROM ref.instruments WHERE is_active = TRUE")
        ).mappings().all()
        return {r["symbol"]: r["instrument_id"] for r in rows}


def upsert_articles(articles):
    """Inserts articles into md.news_articles, skipping duplicates by link."""
    if not articles:
        return 0
    inserted = 0
    with engine.begin() as conn:
        for a in articles:
            try:
                res = conn.execute(
                    text("""
                        INSERT INTO md.news_articles (title, link, published_date, source_name, instrument_id)
                        VALUES (:title, :link, :published_date, :source_name, :instrument_id)
                        ON CONFLICT (link) DO NOTHING
                    """),
                    a,
                )
                inserted += res.rowcount
            except Exception as e:
                logging.error(f"Insert failed for '{a['title'][:50]}': {e}")
    return inserted


# ----- KEYWORD MATCHING -----
def match_article(title, inst_map):
    """
    Returns (symbol, instrument_id) if the title matches any TARGET_KEYWORD.
    Returns None if no match.
    """
    lower = title.lower()
    for sym, keywords in TARGET_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            inst_id = inst_map.get(sym) if sym != "MASI" else None
            return sym, inst_id
    return None


# ----- PARSING -----
def parse_page(html, inst_map):
    """
    Parses a single page of Medias24 bourse news.
    Extracts articles from div.recent-post-info blocks.
    Returns a list of matched article dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Main content articles are in div.recent-post-info
    # The first "hero" article is in div.title-actus-image + div#date-publication
    hero_title_div = soup.find("div", class_="title-actus-image")
    if hero_title_div:
        h1 = hero_title_div.find("h1")
        if h1:
            a_tag = h1.find("a")
            if a_tag:
                title = h1.text.strip()
                link = a_tag.get("href", "")
                # Find date nearby
                date_div = hero_title_div.find_next("div", id="date-publication")
                date_str = ""
                if date_div:
                    span = date_div.find("span", class_="date-post")
                    if span:
                        date_str = span.text.strip().lstrip("| ").strip()

                match = match_article(title, inst_map)
                if match and title and link and date_str:
                    sym, inst_id = match
                    results.append({
                        "title": title,
                        "link": link,
                        "published_date": date_str,
                        "source_name": SOURCE_NAME,
                        "instrument_id": inst_id,
                        "_symbol": sym,
                    })

    # Regular article cards
    blocks = soup.find_all("div", class_="recent-post-info")
    for block in blocks:
        h1 = block.find("h1")
        if not h1:
            continue
        a_tag = h1.find("a")
        if not a_tag:
            continue

        title = h1.text.strip()
        link = a_tag.get("href", "")

        # Date
        date_div = block.find("div", id="date-publication")
        date_str = ""
        if date_div:
            span = date_div.find("span", class_="date-post")
            if span:
                date_str = span.text.strip().lstrip("| ").strip()

        # Skip if missing critical info
        if not title or not link or not date_str:
            continue

        match = match_article(title, inst_map)
        if match:
            sym, inst_id = match
            results.append({
                "title": title,
                "link": link,
                "published_date": date_str,
                "source_name": SOURCE_NAME,
                "instrument_id": inst_id,
                "_symbol": sym,
            })

    return results


# ----- MAIN -----
def scrape_medias24_news():
    print(f"🚀 Starting Medias24 News Scraper ({MAX_PAGES} pages)...")
    inst_map = get_instrument_mapping()
    print(f"   Loaded {len(inst_map)} instrument mappings from DB.")

    driver = None
    total_matched = 0
    total_inserted = 0

    try:
        driver = get_chrome_driver()

        for page_num in range(1, MAX_PAGES + 1):
            url = BASE_URL.format(page=page_num)
            print(f"\n📄 Page {page_num}/{MAX_PAGES}: {url}")

            try:
                driver.get(url)
                time.sleep(2)  # let CloudFlare pass and page render

                html = driver.page_source
                matched = parse_page(html, inst_map)

                if matched:
                    # Remove internal _symbol key before DB insert
                    for m in matched:
                        print(f"   ✅ [{m['_symbol']}] {m['title'][:80]}")
                        del m["_symbol"]

                    inserted = upsert_articles(matched)
                    total_matched += len(matched)
                    total_inserted += inserted
                    print(f"   → {len(matched)} matched, {inserted} new inserted")
                else:
                    print(f"   → 0 relevant articles on this page")

            except Exception as e:
                print(f"   ❌ Error on page {page_num}: {e}")
                logging.error(f"Page {page_num} error: {e}")
                continue

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        logging.error(f"Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()

    print(f"\n✅ Scraping finished! Total matched: {total_matched}, New inserted: {total_inserted}")


if __name__ == "__main__":
    scrape_medias24_news()
