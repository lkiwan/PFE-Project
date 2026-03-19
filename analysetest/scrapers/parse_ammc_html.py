import json
from bs4 import BeautifulSoup

with open('ammc_page_selenium.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')
select = soup.find('select', {'name': 'field_emetteur_target_id_verf'})

company_to_id = {}
if select:
    for opt in select.find_all('option'):
        val = opt.get('value')
        text = opt.text.strip()
        if val and val != 'All':
            company_to_id[text] = val

with open('ammc_companies.json', 'w', encoding='utf-8') as f:
    json.dump(company_to_id, f, ensure_ascii=False, indent=2)

print(f"Saved {len(company_to_id)} companies.")
for name in ["MAROC TELECOM", "CIH", "AKDITAL"]:
    found = False
    for k, v in company_to_id.items():
        if name.lower() in k.lower():
            print(f"Match for {name}: {k} -> {v}")
            found = True
    if not found:
        print(f"No match for {name}")
