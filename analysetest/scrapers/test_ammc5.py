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
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get(url)
time.sleep(5) 

try:
    print("Finding all selects...")
    selects = driver.find_elements(By.TAG_NAME, 'select')
    for sel in selects:
        name = sel.get_attribute('name')
        if name and 'emetteur' in name.lower():
            print(f"Found emetteur select: name={name}, id={sel.get_attribute('id')}")
            options = sel.find_elements(By.TAG_NAME, 'option')
            for opt in options[:20]:
                print(f"  Option: value={opt.get_attribute('value')}, outerHTML={opt.get_attribute('outerHTML').strip()}")
        
except Exception as e:
    print(f"Error: {e}")

try:
    print("\nAttempting to find MAROC TELECOM rows...")
    elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'MAROC')]")
    for elem in elements:
        if elem.tag_name != 'option': # Skip the dropdown options
            print(f"\nFound row element: tag={elem.tag_name}")
            print(f"outerHTML: {elem.get_attribute('outerHTML')[:300]}")
except Exception as e:
    print(f"Error: {e}")

driver.quit()
