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

print(f"Loading {url}...")
driver.get(url)
time.sleep(8) 

print("Iframes on page:")
iframes = driver.find_elements(By.TAG_NAME, 'iframe')
for i, frame in enumerate(iframes):
    print(f"  Iframe {i}: src={frame.get_attribute('src')} id={frame.get_attribute('id')}")

try:
    print("\nAttempting to find elements with text 'MAROC TELECOM'...")
    elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'MAROC')]")
    for elem in elements:
        print(f"Found element: tag={elem.tag_name}, class={elem.get_attribute('class')}, text='{elem.text}'")
except Exception as e:
    print(f"Error finding MAROC: {e}")

try:
    print("\nAttempting to find select with name 'emetteur_ef' (or similar)...")
    selects = driver.find_elements(By.TAG_NAME, 'select')
    for sel in selects:
        print(f"Found select: id={sel.get_attribute('id')}, name={sel.get_attribute('name')}")
except Exception as e:
    print(f"Error finding selects: {e}")

driver.quit()
