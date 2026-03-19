import os
import re
import glob
import pdfplumber
import pandas as pd
import numpy as np
import logging
import urllib.parse

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# --- CONFIGURATION DU DICTIONNAIRE DE MAPPING (REGEX) ---
FINANCIAL_PATTERNS = {
    "chiffre_d_affaires_pnb": [
        r"Produit\s+Net\s+Bancaire", r"\bPNB\b", 
        r"Total\s+des\s+produits\s+d'exploitation", r"Chiffre\s+d'affaires"
    ],
    "resultat_brut_exploitation": [
        r"Résultat\s+Brut\s+d'Exploitation", r"\bRBE\b"
    ],
    "cout_du_risque": [
        r"Coût\s+du\s+risque\s+de\s+crédit", 
        r"Dotations\s+aux\s+provisions\s+pour\s+créances\s+en\s+souffrance",
        r"Dotations\s+nettes\s+aux\s+provisions"
    ],
    "resultat_net_pg": [
        r"Résultat\s+net\s+part\s+du\s+groupe", r"\bRNPG\b",
        r"Résultat\s+net\s+de\s+l'ensemble\s+consolidé\s+-\s+Part\s+du\s+groupe"
    ],
    "total_actif": [
        r"TOTAL\s+ACTIF"
    ],
    "capitaux_propres_pg": [
        r"Capitaux\s+propres\s+-\s+Part\s+du\s+groupe", 
        r"Fonds\s+propres\s+part\s+du\s+groupe"
    ],
    "dettes_financieres": [
        r"Dettes\s+envers\s+la\s+clientèle", 
        r"Dettes\s+de\s+financement"
    ],
    "creances_clients": [
        r"Créances\s+sur\s+la\s+clientèle", 
        r"Prêts\s+et\s+créances\s+sur\s+la\s+clientèle"
    ]
}

def parse_filename_metadata(filename):
    """
    Extrait Ticker, Année et Période depuis le nom du fichier.
    """
    clean_name = urllib.parse.unquote(filename).replace('.pdf', '')
    
    # Année (2013-2026)
    year_match = re.search(r'(20[12]\d)', clean_name)
    year = year_match.group(1) if year_match else "Unknown"
    
    # Période
    period = "FY"
    if any(x in clean_name.upper() for x in ["RFS", "S1", "JUIN", "SEMESTRIEL"]):
        period = "S1"
    
    # Ticker
    prefix = clean_name
    if year_match:
        prefix = clean_name[:year_match.start()]
    
    ticker_parts = re.split(r'[_ \-](RFA|RFS|S1|conso|juin|Bank|Rapport)', prefix, flags=re.IGNORECASE)
    ticker = ticker_parts[0].strip('_ ').upper()
    
    return ticker, year, period

def clean_financial_number(text_value):
    """ Nettoyage rigoureux des formats numériques """
    if not text_value: return np.nan
    
    is_negative = 1
    if '(' in text_value and ')' in text_value: is_negative = -1
    
    clean_val = re.sub(r'[^\d,\.]', '', text_value)
    
    if ',' in clean_val and '.' in clean_val:
        clean_val = clean_val.replace('.', '').replace(',', '.')
    elif ',' in clean_val:
        clean_val = clean_val.replace(',', '.')
        
    try:
        return float(clean_val) * is_negative
    except ValueError:
        return np.nan

def extract_financials_from_pdf(filepath):
    """ Extraction par bloc de texte avec pdfplumber """
    results = {key: np.nan for key in FINANCIAL_PATTERNS.keys()}
    
    try:
        with pdfplumber.open(filepath) as pdf:
            # On lit les 25 premières pages pour couvrir les différents formats
            content = ""
            for i in range(min(25, len(pdf.pages))):
                page_text = pdf.pages[i].extract_text()
                if page_text: content += page_text + "\n"
            
            for key, patterns in FINANCIAL_PATTERNS.items():
                for pattern in patterns:
                    # Regex : Libellé + capture du premier bloc numérique (format financier)
                    regex = rf"{pattern}.*?([\d\s\.,\(\)]{{3,}})"
                    match = re.search(regex, content, re.IGNORECASE | re.MULTILINE)
                    
                    if match:
                        raw_str = match.group(1).strip()
                        num_match = re.search(r'\(?\d[\d\s\.,]+\)?', raw_str)
                        if num_match:
                            val = clean_financial_number(num_match.group(0))
                            if not np.isnan(val):
                                results[key] = val
                                break
            
            if np.isnan(results["cout_du_risque"]): results["cout_du_risque"] = 0.0
                
    except Exception as e:
        logging.error(f"Erreur sur {os.path.basename(filepath)} : {e}")
        
    return results

def run_etl(source_dir, output_csv):
    all_records = []
    pdf_files = glob.glob(os.path.join(source_dir, "*.pdf"))
    
    logging.info(f"Démarrage ETL sur {len(pdf_files)} fichiers...")

    for f in pdf_files:
        filename = os.path.basename(f)
        ticker, year, period = parse_filename_metadata(filename)
        logging.info(f"Traitement : {ticker} | {year} | {period}")
        
        data = extract_financials_from_pdf(f)
        data.update({"ticker": ticker, "annee": year, "periode": period, "source_file": filename})
        all_records.append(data)

    df = pd.DataFrame(all_records)
    cols = ["ticker", "annee", "periode"] + list(FINANCIAL_PATTERNS.keys()) + ["source_file"]
    df = df[cols]
    
    df.to_csv(output_csv, index=False, sep=';', encoding='utf-8-sig')
    logging.info(f"Succès ! Fichier généré : {output_csv}")
    return df

if __name__ == "__main__":
    DIRECTORY = "analysetest/scrapers/AMMC_Resultats"
    OUTPUT = "casablanca_market_historical.csv"
    
    if os.path.exists(DIRECTORY):
        run_etl(DIRECTORY, OUTPUT)
    else:
        logging.error(f"Dossier introuvable : {DIRECTORY}")
