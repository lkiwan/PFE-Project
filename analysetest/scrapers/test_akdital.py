import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import urllib.parse
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

output_dir = os.path.abspath("AMMC_Akdital_Test")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

options = Options()
options.add_argument("--headless=new")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-gpu")
options.add_argument("--user-agent=Mozilla/5.0")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

url = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs?field_emetteur_target_id_verf=46446&field_annee_value_1=All"

try:
    print(f"Loading {url}")
    driver.get(url)
    time.sleep(5)
    
    report_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'espace-emetteurs/etats-financiers/')]")
    print(f"Found {len(report_links)} report links.")
    
    detail_pages = [link.get_attribute('href') for link in report_links]
    
    for detail_url in detail_pages:
        print(f"Visiting {detail_url}")
        driver.get(detail_url)
        time.sleep(3)
        
        pdf_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'sites/default/files')]")
        for pdf_link in pdf_links:
            href = pdf_link.get_attribute('href')
            if not href.startswith('http'):
                href = "https://www.ammc.ma" + href
            
            filename = os.path.basename(urllib.parse.urlparse(href).path)
            print(f"Downloading {filename}")
            
            cookies = driver.get_cookies()
            cookie_dict = {c['name']: c['value'] for c in cookies}
            ua = driver.execute_script("return navigator.userAgent;")
            
            res = requests.get(href, cookies=cookie_dict, headers={"User-Agent": ua}, verify=False, timeout=30)
            if res.status_code == 200:
                with open(os.path.join(output_dir, filename), 'wb') as f:
                    f.write(res.content)
                print(f"Saved {filename}")
            else:
                print(f"Failed {res.status_code}")

finally:
    driver.quit()
