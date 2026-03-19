import os
# Clear any potentially bad environment variables that break requests/webdriver-manager
os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)

import time
from bs4 import BeautifulSoup
import urllib.request

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
except ImportError:
    print("Please install required packages:")
    print("pip install selenium webdriver-manager bs4")
    exit(1)

def download_iam_reports():
    url = "https://www.iam.ma/groupe-maroc-telecom/rapports-publications/rapports-financiers/647526"
    output_dir = "IAM_Resultats"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Starting Selenium to bypass Cloudflare and fetch: {url}")
    
    # Configure Chrome options to auto-download PDFs
    output_dir_abs = os.path.abspath(output_dir)
    options = Options()
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
    
    prefs = {
        "download.default_directory": output_dir_abs,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "plugins.always_open_pdf_externally": True  # By-passes the PDF viewer
    }
    options.add_experimental_option("prefs", prefs)
    print(f"Set download directory to: {output_dir_abs}")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Enforce download behavior via CDP just to be absolutely sure
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": output_dir_abs
    })
    
    try:
        driver.get(url)
        print("Waiting for page and Cloudflare verification to load...")
        
        # Wait up to 30 seconds for the documents container to appear
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "rapport-publication"))
        )
        time.sleep(3) # Extra wait for JS rendering
        
        cards_count = len(driver.find_elements(By.CSS_SELECTOR, 'div.card.rapport-publication'))
        
        if cards_count == 0:
            print("No documents found. Cloudflare might have blocked the session or the page layout changed.")
            return
            
        print(f"Found {cards_count} documents. Starting download via browser clicks...")
        
        for index in range(cards_count):
            try:
                # Re-find the elements to avoid StaleElementReferenceException
                cards = driver.find_elements(By.CSS_SELECTOR, 'div.card.rapport-publication')
                if index >= len(cards):
                    break
                    
                card = cards[index]
                title_elem = card.find_element(By.CSS_SELECTOR, 'h3.card-title')
                button = card.find_element(By.CSS_SELECTOR, 'a.btn-primary')
                
                title = title_elem.text.strip()
                safe_name = "".join([c for c in title if c.isalnum() or c in ' -_']).strip()
                if not safe_name:
                    safe_name = f"IAM_Report_{index}"
                if not safe_name.lower().endswith('.pdf'):
                    safe_name += '.pdf'
                    
                print(f"Downloading: {safe_name}")
                
                # Get the link href
                download_link = button.get_attribute('href')
                if not download_link.startswith('http'):
                    download_link = "https://www.iam.ma" + download_link
                    
                print(f"Downloading: {safe_name}")
                print(f"   -> URL: {download_link}")
                
                # Navigate directly to the link to trigger the download manager
                driver.get(download_link)
                print("   -> Navigated to download link. Waiting for completion...")
                
                # Wait for the download to finish
                timeout = 120
                start_time = time.time()
                while time.time() - start_time < timeout:
                    crdownloads = [f for f in os.listdir(output_dir) if f.endswith('.crdownload')]
                    if not crdownloads:
                        # Check if any new file appeared
                        all_files = [f for f in os.listdir(output_dir)]
                        if any(f != safe_name for f in all_files):
                            break
                    time.sleep(1)
                
                # The file might be named 'downloads.htm' or something generic.
                # Let's find the most recently created file in the directory that is NOT a .crdownload
                # and rename it to safe_name.
                all_files = [f for f in os.listdir(output_dir) if not f.endswith('.crdownload')]
                if all_files:
                    # Sort files by modification time
                    all_files_paths = [os.path.join(output_dir, f) for f in all_files]
                    newest_file = max(all_files_paths, key=os.path.getmtime)
                    newest_filename = os.path.basename(newest_file)
                    
                    if newest_filename != safe_name:
                        target_path = os.path.join(output_dir, safe_name)
                        if os.path.exists(target_path):
                            os.remove(target_path)
                        os.rename(newest_file, target_path)
                        print(f"   -> Successfully downloaded and renamed to: {safe_name}")
                    else:
                        print(f"   -> Successfully downloaded: {safe_name}")
                else:
                    print("   -> Download timed out or failed.")
                
            except Exception as e:
                print(f"   -> Error on document {index}: {e}")
                
            time.sleep(2) # Polite delay
            
        print("Waiting 5 seconds before closing...")
        time.sleep(5)
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()
        print(f"\nProcess finished. Files should be saved in: {output_dir_abs}")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    download_iam_reports()
