import requests
from bs4 import BeautifulSoup
import os

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

url = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs"
r = requests.get(url, verify=False)
soup = BeautifulSoup(r.content, 'html.parser')

with open("ammc_page.html", "w", encoding="utf-8") as f:
    f.write(soup.prettify())

print("Saved ammc_page.html")
