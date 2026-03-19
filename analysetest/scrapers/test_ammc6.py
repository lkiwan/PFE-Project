import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

url = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs"

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--user-agent=Mozilla/5.0")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
driver.get(url)
time.sleep(4) 

try:
    selects = driver.find_elements(By.TAG_NAME, 'select')
    for sel in selects:
        if 'emetteur' in str(sel.get_attribute('name')).lower():
            opts = sel.find_elements(By.TAG_NAME, 'option')
            for o in opts:
                text = o.text.upper()
                if "CIH" in text or "CREDIT IMMOBILIER" in text or "MAROC TELECOM" in text:
                    print(f"FOUND: {text.strip()} -> ID: {o.get_attribute('value')}")
            break
except Exception as e:
    print(f"Error: {e}")

driver.quit()
