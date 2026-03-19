import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

url = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs"

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get(url)
time.sleep(5) # Let JS render the form

soup = BeautifulSoup(driver.page_source, 'html.parser')
options_dict = {}

form = soup.find('form', id='views-exposed-form-etats-financiers-emetteur-page-1')
if form:
    select = form.find('select', {'name': 'emetteur_ef'})
    if select:
        for opt in select.find_all('option'):
            if opt.get('value') and opt.get('value') != 'All':
                options_dict[opt.text.strip()] = opt.get('value')

with open("ammc_companies.json", "w", encoding="utf-8") as f:
    json.dump(options_dict, f, ensure_ascii=False, indent=2)

print(f"Saved {len(options_dict)} companies to ammc_companies.json")
driver.quit()
