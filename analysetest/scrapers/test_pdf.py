import requests
import urllib3
urllib3.disable_warnings()

url = "https://www.iam.ma/documents/66341/0/Maroc+Telecom+-+Rapport+financier+2025+%281%29.pdf/0d3b0317-f0fe-8d99-99d3-7ecef52e0bdd?t=1772025169628"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "max-age=0"
}
try:
    r = requests.get(url, headers=headers, verify=False, timeout=10)
    print("Status Code:", r.status_code)
    print("Content-Type:", r.headers.get("Content-Type"))
    if r.status_code == 200 and "application/pdf" in r.headers.get("Content-Type", ""):
        print("Success: PDF is downloadable via simple requests!")
    else:
        print("Failed: Cloudflare blocking the PDF as well.")
except Exception as e:
    print("Error:", e)
