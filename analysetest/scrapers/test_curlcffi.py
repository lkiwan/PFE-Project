import os
from curl_cffi import requests

os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

url = "https://www.iam.ma/groupe-maroc-telecom/rapports-publications/rapports-financiers/647526"
pdf_url = "https://www.iam.ma/documents/66341/0/Maroc+Telecom+-+Rapport+financier+2025+%281%29.pdf/0d3b0317-f0fe-8d99-99d3-7ecef52e0bdd?t=1772025169628"

print("Fetching main page to get Cloudflare clearance...")
session = requests.Session(impersonate="chrome110")
resp = session.get(url, verify=False)
print("Page Status:", resp.status_code)

print("Fetching PDF...")
pdf_resp = session.get(pdf_url, verify=False)
print("PDF Status:", pdf_resp.status_code)
print("PDF Length:", len(pdf_resp.content))

if len(pdf_resp.content) > 10000: # Actually a PDF and not a CF block page
    with open("iam_test.pdf", "wb") as f:
        f.write(pdf_resp.content)
    print("Success! Saved as iam_test.pdf")
else:
    print("Failed to bypass Cloudflare. Content size too small.")
