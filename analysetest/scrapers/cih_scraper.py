import os
import requests
import urllib3
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_financial_results():
    base_url = "https://cihbank.ma"
    url = f"{base_url}/espace-financier/resultats-financiers"
    output_dir = "CIH_Resultats"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Fetching page: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the page: {e}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    
    # PDF links are inside <button onclick="window.location.href='...'">Télécharger</button>
    # The titles are usually in the proceeding <h1> block inside the same <div class="box">
    
    pdf_links = []
    
    # Find all divs with class 'box' which holds the financial record
    boxes = soup.find_all('div', class_='box')
    
    for box in boxes:
        title_tag = box.find('h1')
        button_tag = box.find('button')
        
        if title_tag and button_tag and button_tag.has_attr('onclick'):
            title = title_tag.text.strip()
            onclick_attr = button_tag['onclick']
            
            # Extract URL from window.location.href='/url/to/file.pdf'
            match = re.search(r"window\.location\.href='([^']+)'", onclick_attr)
            if match:
                href = match.group(1)
                full_url = urljoin(base_url, href)
                pdf_links.append((title, full_url))

    unique_links = {url: name for name, url in pdf_links}
    
    if not unique_links:
        print("No documents found on the page.")
        return
        
    print(f"Found {len(unique_links)} documents. Starting download...")
    
    for link_url, name in unique_links.items():
        safe_name = "".join([c for c in name if c.isalnum() or c in ' -_']).strip()
        if not safe_name:
            safe_name = link_url.split('/')[-1].split('?')[0]
            
        if not safe_name.lower().endswith('.pdf'):
            safe_name += '.pdf'
            
        file_path = os.path.join(output_dir, safe_name)
        
        print(f"Downloading: {safe_name}")
        try:
            pdf_response = requests.get(link_url, headers=headers, timeout=20, verify=False)
            pdf_response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(pdf_response.content)
            print(f"  -> Saved to {file_path}")
        except Exception as e:
            print(f"  -> Failed to download: {e}")

    print("\nDownload completed successfully!")

if __name__ == "__main__":
    download_financial_results()
