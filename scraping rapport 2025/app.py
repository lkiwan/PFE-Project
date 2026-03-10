import requests
from bs4 import BeautifulSoup
import os
import urllib3

# Disable the SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.ammc.ma/fr/liste-etats-financiers-emetteurs"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def fetch_available_companies():
    """Scrapes the AMMC page to build a live dictionary of all listed companies."""
    print("[*] Initializing database connection... Fetching company list from AMMC...")
    
    response = requests.get(BASE_URL, headers=HEADERS, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    all_selects = soup.find_all('select')
    select_box = None
    
    # Length Heuristic: Find the dropdown with the Moroccan companies
    for box in all_selects:
        if len(box.find_all('option')) > 50:
            select_box = box
            break
            
    if not select_box:
        print("[-] Error: Could not locate the company dropdown.")
        return None, None

    select_name = select_box.get('name') 
    
    company_ids = {}
    for option in select_box.find_all('option'):
        if option.get('value'): 
            # Store as { "COMPANY NAME": "ID" }
            company_ids[option.text.strip().upper()] = option.get('value')
            
    return company_ids, select_name

def download_ammc_report(matched_id, matched_name, select_name, target_year="2024", save_directory="BVC_Data"):
    """Handles the actual scanning and downloading of the PDF."""
    search_url = f"{BASE_URL}?{select_name}={matched_id}"
    print(f"\n[*] Scanning AMMC database for {matched_name} ({target_year} Reports)...")
    
    doc_response = requests.get(search_url, headers=HEADERS, verify=False)
    doc_soup = BeautifulSoup(doc_response.content, 'html.parser')

    rows = doc_soup.find_all('tr')
    pdf_link = None
    keywords = ["rfa", "annuel", "financier", "rapport", "etats"]

    for row in rows:
        row_text = row.text.lower()
        
        if target_year in row_text and any(k in row_text for k in keywords):
            link_tag = row.find('a', href=True)
            if link_tag:
                href = link_tag['href']
                if not href.startswith('http'):
                    href = "https://www.ammc.ma" + href
                
                if href.lower().endswith('.pdf'):
                    pdf_link = href
                    break
                else:
                    # Deep Crawl
                    print(f"[*] Report sub-page detected. Navigating deeper...")
                    node_response = requests.get(href, headers=HEADERS, verify=False)
                    node_soup = BeautifulSoup(node_response.content, 'html.parser')
                    
                    pdf_tag = node_soup.find('a', href=lambda x: x and x.lower().endswith('.pdf'))
                    if pdf_tag:
                        pdf_link = pdf_tag['href']
                        if not pdf_link.startswith('http'):
                            pdf_link = "https://www.ammc.ma" + pdf_link
                        break

    if not pdf_link:
        print(f"[-] No {target_year} annual report found for {matched_name}.")
        return

    print(f"[+] {target_year} PDF located! URL: {pdf_link}")

    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    safe_name = matched_name.replace(" ", "_").replace("/", "-")
    file_path = os.path.join(save_directory, f"{safe_name}_RFA_{target_year}.pdf")

    print(f"[*] Downloading PDF to your PFE workspace... Please wait.")
    pdf_data = requests.get(pdf_link, headers=HEADERS, verify=False)

    with open(file_path, 'wb') as file:
        file.write(pdf_data.content)

    print(f"[SUCCESS] File saved locally at: {file_path}")

# ==========================================
# INTERACTIVE TERMINAL INTERFACE
# ==========================================
if __name__ == "__main__":
    companies_dict, html_param = fetch_available_companies()
    
    if companies_dict:
        print("\n" + "="*50)
        print(" BOURSE DE CASABLANCA - ENTREPRISES DISPONIBLES")
        print("="*50)
        
        # Sort and print all available companies in a clean list
        sorted_companies = sorted(companies_dict.keys())
        for i, company in enumerate(sorted_companies, 1):
            print(f"  {company}")
            
        print("="*50)
        
        # Ask the user for input
        while True:
            user_choice = input("\nTapez le nom de l'entreprise (ou une partie du nom) : ").strip().upper()
            
            # Find the match based on user input
            matched_id, matched_name = None, None
            for name, comp_id in companies_dict.items():
                if user_choice in name:
                    matched_id = comp_id
                    matched_name = name
                    break
            
            if matched_id:
                target_year = input(f"Tapez l'année souhaitée pour {matched_name} (ex: 2024) : ").strip()
                download_ammc_report(matched_id, matched_name, html_param, target_year)
                break
            else:
                print("[-] Entreprise introuvable. Veuillez réessayer avec un nom valide de la liste ci-dessus.")