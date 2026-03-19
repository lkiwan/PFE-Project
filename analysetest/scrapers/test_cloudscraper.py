import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import cloudscraper

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

def test_hybrid():
    url = "https://www.iam.ma/groupe-maroc-telecom/rapports-publications/rapports-financiers/647526"
    pdf_url = "https://www.iam.ma/documents/66341/0/Maroc+Telecom+-+Rapport+financier+2025+%281%29.pdf/0d3b0317-f0fe-8d99-99d3-7ecef52e0bdd?t=1772025169628"
    
    options = Options()
    # No headless to pass cloudflare easier
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    print("Fetching Cloudflare cookie via Selenium...")
    driver.get(url)
    time.sleep(10)
    
    cookies = driver.get_cookies()
    user_agent = driver.execute_script("return navigator.userAgent;")
    driver.quit()
    
    print(f"Got {len(cookies)} cookies.")
    print(f"User Agent: {user_agent}")
    
    # Now try cloudscraper
    print("\nAttempting cloudscraper with cookies...")
    scraper = cloudscraper.create_scraper(browser={'custom': user_agent})
    for c in cookies:
        scraper.cookies.set(c['name'], c['value'])
        
    res = scraper.get(pdf_url, verify=False)
    print(f"Cloudscraper Res: {res.status_code}, Length: {len(res.content)}")
    if len(res.content) > 10000:
        print("Success!")
    else:
        print("Failed (probably CF Block page).")

if __name__ == "__main__":
    test_hybrid()
