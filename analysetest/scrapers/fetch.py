import requests
import urllib3
urllib3.disable_warnings()

url = "https://www.iam.ma/groupe-maroc-telecom/rapports-publications/rapports-financiers/647526"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "max-age=0"
}

r = requests.get(url, headers=headers, verify=False)
print("Status Code:", r.status_code)
if r.status_code == 200:
    with open("iam_raw.html", "w", encoding="utf-8") as f:
        f.write(r.text)
    print("Saved to iam_raw.html")
else:
    print(r.text[:500])
