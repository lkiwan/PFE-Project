import os
import re
import shutil

# Path to the directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AMMC_DIR = os.path.join(BASE_DIR, "AMMC_Resultats")

# Mapping of keywords to Tickers (expanded based on existing scripts)
TICKER_MAP = {
    "MAROC TELECOM": "IAM",
    "IAM": "IAM",
    "ITISSALAT": "IAM",
    "ATTIJARI": "ATW",
    "ATW": "ATW",
    "AWB": "ATW",
    "BCP": "BCP",
    "BOA": "BOA",
    "BANK OF AFRICA": "BOA",
    "BMCE": "BOA",
    "CIH": "CIH",
    "CREDIT IMMOBILIER": "CIH",
    "COSUMAR": "COSUMAR",
    "LAFARGE": "LHM",
    "CIMENTS": "CIM", # ambiguous but common
    "TAQA": "TAQA",
    "TOTAL": "TTE",
    "TOTALENERGIES": "TTE",
    "AFRIQUIA": "AFG",
    "GAZ": "AFG",
    "MANAGEM": "MNG",
    "SNEP": "SNEP",
    "SONASID": "SID",
    "DOUJA": "ADH",
    "ADDOHA": "ADH",
    "ALLIANCES": "ADI",
    "ADI": "ADI",
    "RDS": "RDS",
    "RESIDENCES": "RDS",
    "JET": "JET",
    "CONTRACTORS": "JET",
    "ALUMINIUM": "ALM",
    "MAROC LEASING": "MLE",
    "MAROC_LEASING": "MLE",
    "SALAFIN": "SLF",
    "EQDOM": "EQD",
    "MAGHREBAIL": "MAB",
    "MUTANDIS": "MUT",
    "AKDITAL": "AKT",
    "TGCC": "TGCC",
    "ARADEI": "ARD",
    "IMMORENTE": "IMO",
    "SOTHEMA": "SOT",
    "PROMOPHARM": "PRO",
    "LABEL": "LBV",
    "VIE": "LBV",
    "HPS": "HPS",
    "DISWAY": "DWY",
    "MICRODATA": "MIC",
    "M2M": "M2M",
    "INVOLYS": "INV",
    "RISMA": "RIS",
    "CTM": "CTM",
    "TIMAR": "TIM",
    "MARSA": "MSA",
    "SODEP": "MSA",
    "ATLANTASANAD": "ATL",
    "ATLANTA": "ATL",
    "SANAD": "ATL",
    "AFMA": "AFM",
    "AGMA": "AGM",
    "WAFA": "WAA",
    "ASSURANCE": "WAA", # Risky
    "SAHAM": "SAH",
    "SANLAM": "SAH",
    "DELATTRE": "DLM",
    "LEVARA": "DLM",
    "STRY": "STR",
    "IB": "IB",
    "MAROC": "IAM", # Last resort
}

def get_ticker(filename):
    upper_name = filename.upper()
    
    # Check strict matches first
    for key, ticker in TICKER_MAP.items():
        # Word boundary check to avoid partial matches like "DI" in "ADI"
        if re.search(r'\b' + re.escape(key) + r'\b', upper_name):
            return ticker
            
    # Fallback to loose containment
    for key, ticker in TICKER_MAP.items():
        if key in upper_name:
            return ticker
            
    return "UNKNOWN"

def get_year(filename):
    match = re.search(r'(20\d{2})', filename)
    if match:
        return match.group(1)
    return "0000"

def get_period(filename):
    lower_name = filename.lower()
    if 's1' in lower_name or 'semestriel' in lower_name or 'semestre 1' in lower_name or '30 juin' in lower_name:
        return "s1"
    # Assume annual otherwise (as per requirement: "IAM-2020 for annual reports")
    return "annual"

def rename_files():
    if not os.path.exists(AMMC_DIR):
        print(f"Directory {AMMC_DIR} does not exist.")
        return

    files = [f for f in os.listdir(AMMC_DIR) if f.endswith('.pdf')]
    print(f"Found {len(files)} PDF files in {AMMC_DIR}")

    count = 0
    for filename in files:
        ticker = get_ticker(filename)
        year = get_year(filename)
        period = get_period(filename)

        if ticker == "UNKNOWN":
            print(f"Skipping {filename}: Could not determine ticker.")
            continue
        
        if year == "0000":
            print(f"Skipping {filename}: Could not determine year.")
            continue

        if period == "s1":
            new_name = f"{ticker}-s1-{year}.pdf"
        else:
            new_name = f"{ticker}-{year}.pdf"

        old_path = os.path.join(AMMC_DIR, filename)
        new_path = os.path.join(AMMC_DIR, new_name)

        if old_path != new_path:
            try:
                # Handle overwrites or duplicates
                if os.path.exists(new_path):
                    print(f"Target exists: {new_name}. Checking sizes...")
                    if os.path.getsize(old_path) > os.path.getsize(new_path):
                         print(f"Overwriting smaller file {new_name} with {filename}")
                         shutil.move(old_path, new_path)
                         count += 1
                    else:
                        print(f"Keeping existing larger/equal file {new_name}, deleting {filename}")
                        os.remove(old_path)
                else:
                    os.rename(old_path, new_path)
                    print(f"Renamed: {filename} -> {new_name}")
                    count += 1
            except Exception as e:
                print(f"Error renaming {filename}: {e}")
    
    print(f"Renamed {count} files.")

if __name__ == "__main__":
    rename_files()
