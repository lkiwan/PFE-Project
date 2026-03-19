import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

url = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs?field_emetteur_target_id_verf=46446&field_annee_value_1=All"

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--user-agent=Mozilla/5.0")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get(url)
time.sleep(5) 

try:
    print("Finding elements with 'Rapport' text...")
    elements = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'RAPPORT', 'rapport'), 'rapport')]")
    for elem in elements:
        print(f"Tag: {elem.tag_name}, text: {elem.text}")
        if elem.tag_name in ['a', 'div', 'span', 'td']:
            print(f"Parent HTML: {elem.find_element(By.XPATH, '..').get_attribute('outerHTML')[:400]}")
            print("---")
            
except Exception as e:
    print(f"Error: {e}")

# Lets look for all links in the main content area with "rapports" in the href
try:
    links = driver.find_elements(By.XPATH, "//a[contains(@href, 'espace-emetteurs/etats-financiers')]")
    for link in links:
        print(f"Found link: {link.get_attribute('href')}")
except Exception as e:
    pass

driver.quit()
