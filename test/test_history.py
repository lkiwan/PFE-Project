import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import os
import certifi
from webdriver_manager.chrome import ChromeDriverManager

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

print("Fetching history for aktidal...")
driver.get("https://medias24.com/leboursier/fiche-action?action=aktidal&valeur=historiques")
time.sleep(10) # wait long enough for any XHR to finish

with open("test_history.html", "w", encoding="utf-8") as f:
    f.write(driver.page_source)

print("Saved to test_history.html")
driver.quit()
