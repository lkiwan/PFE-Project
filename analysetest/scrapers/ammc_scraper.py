import os
import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import urllib.parse
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Clear custom cert vars that might cause issues with webdriver-manager
os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

def download_ammc_reports(target_companies, output_base_dir, force_mapping=None):
    """
    Scrapes the AMMC website for targeted companies,
    extracts the links to their financial reports, and downloads the PDFs.
    """
    output_dir = os.path.abspath(output_base_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Initialize Chrome options for Selenium
    options = Options()
    options.add_argument("--headless=new") 
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    base_url = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs"
    
    # Fallback/Default mapping
    company_to_id = force_mapping if force_mapping else {
        "MAROC TELECOM": "2798",
        "CIH": "2757",
        "AKDITAL": "46446"
    }

    try:
        for company_name in target_companies:
            print(f"\n==============================================")
            print(f"Processing company: {company_name}")
            print(f"==============================================")
            
            target_id = company_to_id.get(company_name)
            if not target_id:
                # One last attempt to find ID on the live page if mapping is missing
                print(f"  -> ID missing for {company_name}, attempt to find on page...")
                driver.get(base_url)
                time.sleep(3)
                try:
                    from selenium.webdriver.support.ui import Select
                    select = Select(driver.find_element(By.XPATH, "//select[contains(@name, 'emetteur')]"))
                    for opt in select.options:
                        if company_name.lower() in opt.text.lower():
                            target_id = opt.get_attribute('value')
                            print(f"  -> Found ID dynamically: {target_id}")
                            break
                except:
                    pass
                    
            if not target_id:
                print(f"  -> Skipping {company_name}, could not find matching ID.")
                continue
                
            filtered_url = f"https://www.ammc.ma/fr/liste-etats-financiers-emetteurs?field_emetteur_target_id_verf={target_id}&field_annee_value_1=All"
            print(f"  -> Loading filtered URL: {filtered_url}")
            driver.get(filtered_url)
            time.sleep(5) # Wait for page reload
            
            # Find all result links to report detail pages
            report_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'espace-emetteurs/etats-financiers/')]")
            
            detail_pages = []
            for link in report_links:
                href = link.get_attribute('href')
                if href and href not in detail_pages:
                    detail_pages.append(href)
                    
            print(f"  -> Found {len(detail_pages)} report detail pages.")
            
            # Now visit each detail page and download the PDF
            for detail_url in detail_pages:
                print(f"\n  -> Visiting detail page: {detail_url}")
                driver.get(detail_url)
                time.sleep(3)
                
                # The actual PDF links are hosted under sites/default/files
                pdf_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'sites/default/files')]")
                
                if not pdf_links:
                    print("  -> No PDF found on this detail page.")
                    continue
                    
                for pdf_link in pdf_links:
                    pdf_href = pdf_link.get_attribute('href')
                    if not pdf_href.startswith('http'):
                        pdf_href = "https://www.ammc.ma" + pdf_href
                        
                    filename = os.path.basename(urllib.parse.urlparse(pdf_href).path)
                    print(f"  -> Attempting to download PDF: {filename}")
                    
                    # Let's get the absolute path of the expected file
                    expected_file_path = os.path.join(output_dir, filename)
                    
                    # If it already exists, remove it
                    if os.path.exists(expected_file_path):
                        os.remove(expected_file_path)
                    
                    # Extract Selenium cookies to reuse clearance
                    cookies = driver.get_cookies()
                    cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
                    user_agent = driver.execute_script("return navigator.userAgent;")
                    
                    print(f"  -> Fetching PDF via requests...")
                    
                    headers = {
                        "User-Agent": user_agent,
                        "Referer": detail_url
                    }
                    
                    try:
                        response = requests.get(
                            pdf_href, 
                            cookies=cookie_dict, 
                            headers=headers,
                            timeout=60,
                            verify=False
                        )
                        
                        if response.status_code == 200:
                            # Verify if we actually got a PDF or just another Cloudflare HTML page
                            content_type = response.headers.get('Content-Type', '')
                            if 'application/pdf' in content_type.lower() or response.content[:4] == b'%PDF':
                                with open(expected_file_path, 'wb') as f:
                                    f.write(response.content)
                                print(f"  -> Successfully downloaded: {filename}")
                            else:
                                print(f"  -> Failed: Server returned HTML instead of PDF. Cloudflare block still active.")
                                # Save it anyway for debugging
                                debug_path = os.path.join(output_dir, filename + ".html")
                                with open(debug_path, 'wb') as f:
                                    f.write(response.content)
                                print(f"  -> Saved debug HTML to {debug_path}")
                        else:
                            print(f"  -> Failed to download: HTTP {response.status_code}")
                            
                    except Exception as e:
                        print(f"  -> Error fetching PDF: {e}")
                        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nClosing Chrome...")
        driver.quit()

if __name__ == "__main__":
    # Portfolio symbols from db/reseed_db.py + common ones
    target_symbols = [
        'AKDITAL', 'ALLIANCES', 'ATW', 'CIH', 'ADDOHA-P', 
        'IAM', 'JET CONTRACTORS', 'SGTM', 'SODEP', 'TAQA', 'TGCC'
    ]
    
    # Internal mapping for symbols that don't match or have specific AMMC names
    alias_map = {
        "IAM": "MAROC TELECOM",
        "ATW": "ATTIJARIWAFA BANK",
        "ADDOHA-P": "ADDOHA",
        "SODEP": "MARSA MAROC",
        "ALLIANCES": "Alliances Développement Immobilier",
        "TAQA": "Taqa Morocco"
    }
    
    # Final list for the scraper (converted to AMMC recognizable names where possible)
    target_companies = []
    for s in target_symbols:
        target_companies.append(alias_map.get(s, s))

    output_base_dir = "AMMC_Resultats"
    
    # Load mapping from JSON if it exists
    json_path = os.path.join(os.path.dirname(__file__), "ammc_companies.json")
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            full_mapping = json.load(f)
            
        print(f"Loaded {len(full_mapping)} company mappings from JSON.")
        
        # We can also just iterate through our target names and find IDs
        identified_targets = {}
        for target_name in target_companies:
            found = False
            # Try exact match
            if target_name in full_mapping:
                identified_targets[target_name] = full_mapping[target_name]
                found = True
            else:
                # Try partial match
                for ammc_name, ammc_id in full_mapping.items():
                    if target_name.upper() in ammc_name.upper():
                        print(f"  -> Mapping '{target_name}' to AMMC name: '{ammc_name}' (ID: {ammc_id})")
                        identified_targets[ammc_name] = ammc_id
                        found = True
                        break
            if not found:
                print(f"  -> Warning: Could not find mapping for '{target_name}'")

        # Now run targeting the specific AMMC names
        download_ammc_reports(list(identified_targets.keys()), output_base_dir, force_mapping=identified_targets)
    else:
        print("Warning: ammc_companies.json not found. Using partial hardcoded fallbacks.")
        download_ammc_reports(target_companies, output_base_dir)
