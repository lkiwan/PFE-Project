import os
import time
import certifi
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

options = Options()
options.add_argument("--headless=new")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

try:
    print("Fetching page 1...")
    driver.get("https://medias24.com/categorie/leboursier/bourse-leboursier/page/1/")
    time.sleep(4)
    
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    
    # Save full HTML for offline debugging
    with open("medias24_news_debug.html", "w", encoding="utf-8") as f:
        f.write(html)
    
    # Find ALL h2/h3 headings that contain links
    headings = soup.find_all(["h2", "h3"])
    found = 0
    for h in headings:
        a = h.find("a")
        if a and a.get("href"):
            href = a.get("href")
            print("---")
            print("Title:", h.text.strip())
            print("Link:", href)
            parent = h.find_parent("div")
            if parent:
                print("Parent classes:", parent.get("class", []))
            found += 1
            if found >= 5:
                break
    
    if found == 0:
        # Try finding all anchor tags
        all_links = soup.find_all("a")
        print(f"\nTotal <a> tags: {len(all_links)}")
        for link in all_links[:20]:
            href = link.get("href", "")
            txt = link.text.strip()
            if txt and len(txt) > 20:
                print(f"  Text: {txt[:80]}  |  href: {href}")
    
    # Pagination info
    print("\n=== PAGINATION ===")
    page_links = soup.find_all("a", class_="page-numbers")
    for p in page_links:
        print(f"  Text: {p.text.strip()}, href: {p.get('href')}")

finally:
    driver.quit()
