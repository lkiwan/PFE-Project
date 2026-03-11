import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://medias24.com/leboursier/fiche-action?action=akdital&valeur=historiques"
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
soup = BeautifulSoup(r.text, 'html.parser')

tables = soup.find_all('table')
print(f"Total tables found: {len(tables)}")

for i, t in enumerate(tables):
    try:
        df = pd.read_html(str(t))[0]
        print(f"--- Table {i} ---")
        print("Classes:", t.get('class'))
        print(df.head(2))
    except Exception as e:
        print(f"Table {i} parse error: {e}")
