import requests

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://medias24.com/leboursier/fiche-action?action=akdital&valeur=historiques"
}

url_hist = "https://medias24.com/content/api?method=getStockHistory&ISIN=akdital&format=json"
r_hist = requests.get(url_hist, headers=headers, verify=False)
print("History Status:", r_hist.status_code)
print(r_hist.text[:500])

url_ob = "https://medias24.com/content/api?method=getBidAsk&ISIN=akdital&format=json"
r_ob = requests.get(url_ob, headers=headers, verify=False)
print("Orderbook Status:", r_ob.status_code)
print(r_ob.text[:500])
