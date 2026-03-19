import os
import re
import pandas as pd
import pdfplumber
from pathlib import Path
from typing import Dict, List, Optional, Union

# Configuration
AMMC_RESULTS_DIR = Path("analysetest/scrapers/AMMC_Resultats")
OUTPUT_DIR = Path("AMMC_Resultats_Extracted")

# Metrics to extract with extended aliases
METRICS_MAPPING = {
    "total_actif": [r"\btotal\s+actif\b", r"total\s+de\s+l['’]actif"],
    "capitaux_propres_pg": [r"capitaux\s+propres\s*[-–]\s*part\s+du\s+groupe", r"capitaux\s+propres\s+pg", r"total\s+capitaux\s+propres", r"situation\s+nette\s+part\s+du\s+groupe"],
    "dettes_financieres_lt": [r"dette[s]?\s+financi[eè]re[s]?\s+non\s+courante[s]?", r"dette[s]?\s+financi[eè]re[s]?\s+long\s+terme", r"passifs\s+financi[eè]rs\s+non\s+courants"],
    "dettes_financieres_ct": [r"dette[s]?\s+financi[eè]re[s]?\s+courante[s]?", r"dette[s]?\s+financi[eè]re[s]?\s+court\s+terme", r"passifs\s+financi[eè]rs\s+courants", r"concours\s+bancaires"],
    "tresorerie_nette": [r"tr[eé]sorerie\s+nette", r"position\s+de\s+tr[eé]sorerie\s+nette", r"tr[eé]sorerie\s+active\s+et\s+passive"],
    "creances_clients": [r"cr[eé]ances\s+clients?", r"cr[eé]ances\s+client[eè]le", r"clients\s+et\s+comptes\s+rattach[eé]s"],
    "chiffre_d_affaires_pnb": [r"chiffre\s+d['’]affaires", r"produit\s+net\s+bancaire", r"\bPNB\b", r"revenus\s+consolid[eé]s"],
    "resultat_brut_exploitation": [r"r[eé]sultat\s+brut\s+d['’]exploitation", r"rbe\b", r"ebitda", r"excl\.\s+dotations"],
    "resultat_net_pg": [r"r[eé]sultat\s+net\s+part\s+du\s+groupe", r"r[eé]sultat\s+net\s+pg", r"rnpg", r"b[eé]n[eé]fice\s+net\s+part\s+du\s+groupe"],
    "cout_du_risque": [r"co[uû]t\s+du\s+risque", r"dotations\s+aux\s+provisions\s+pour\s+cr[eé]ances"],
    "dotations_amortissements": [r"dotations?\s+aux\s+amortissements", r"amortissements\s+et\s+provisions", r"dotations?\s+d['’]exploitation"],
    "flux_tresorerie_operationnel": [r"flux.*?tr[eé]sorerie.*?activit[eé]s?\s+op[eé]rationnelles", r"cash-flow\s+op[eé]rationnel", r"caf", r"capacit[eé]\s+d['’]autofinancement"],
    "capex": [r"capex", r"d[eé]penses?\s+d['’]investissement", r"investissements?\s+corporels", r"acquisition\s+d['’]immobilisations"],
    "nombre_actions_circulation": [r"nombre\s+d['’]actions\s+en\s+circulation", r"nombre\s+total\s+d['’]actions", r"nombre\s+d['’]actions\s+composant\s+le\s+capital"],
    "dividende_par_action": [r"dividende\s+par\s+action", r"DPA\b", r"dividende\s+au\s+titre\s+de\s+l['’]exercice"],
    "prix_cloture": [r"cours\s+de\s+cl[oô]ture", r"prix\s+de\s+l['’]action"]
}

def get_metadata_from_filename(filename):
    """
    Extracts metadata from filename formatted as TICKER-s1-YEAR.pdf or TICKER-YEAR.pdf
    """
    name = filename.replace(".pdf", "")
    parts = name.split("-")
    
    # Heuristic: Ticker is first, Year is last
    ticker = parts[0]
    year_str = parts[-1]
    
    # Handle cases where year might be mixed (e.g. IAM-2019) or IAM-s1-2020
    if not year_str.isdigit():
        # Fallback search for 4 digit year
        match = re.search(r"(20\d{2})", name)
        if match:
            year = int(match.group(1))
        else:
            year = 0
    else:
        year = int(year_str)
    
    if "s1" in filename.lower():
        trimestre = "S1"
    else:
        trimestre = "Annuel"
        
    return ticker, year, trimestre

def _norme_comptable_from_year(year: int) -> str:
    return "Marocaine" if year < 2018 else "IFRS"

def _parse_number(text: str) -> Optional[float]:
    """
    Parses a number string like "13 042", "-13 872", "(1 234)", "12,5"
    """
    if not text:
        return None
    
    # Remove standard whitespace
    clean_text = text.replace(" ", "").replace("\u00a0", "")
    
    # Handle parenthesis as negative: (123) -> -123
    if clean_text.startswith("(") and clean_text.endswith(")"):
        clean_text = "-" + clean_text[1:-1]
    
    # Handle comma decimals: 12,5 -> 12.5
    clean_text = clean_text.replace(",", ".")
    
    # Remove any other non-numeric chars except minus and dot
    # Be careful not to remove 'e' if scientific notation (unlikely here)
    clean_text = re.sub(r"[^\d.-]", "", clean_text)
    
    try:
        return float(clean_text)
    except ValueError:
        return None

def _find_value_in_line(line: str) -> Optional[float]:
    # Heuristic: Split by "  " (double space) first? 
    # PDF text extraction often preserves visual gaps as multiple spaces.
    parts = re.split(r'\s{2,}', line.strip())
    
    candidates = []
    
    # If split didn't work (single spaces), we fall back to regex
    if len(parts) < 2:
        # Fallback: look for patterns of numbers.
        # We will assume that financial numbers are usually > 100 or have specific formatting.
        
        # Regex to find "N NNN" blocks (space separated thousands).
        # UPDATED: Capture optional negative sign at the start!
        # (?<![\d.]) : Ensure we aren't in the middle of a number (like 1.234)
        # (?:\-?\s*)? : Optional minus sign, optionally followed by space
        # \d{1,3} : 1-3 digits
        # (?:[\s\u00a0]\d{3})+ : At least one group of 3 digits separated by space
        # (?:,\d+)? : Optional decimal part
        p = r"(?<![\d.])(?:\-?\s*)?\d{1,3}(?:[\s\u00a0]\d{3})+(?:,\d+)?(?!\d)"
        matches = re.findall(p, line)
        if matches:
             # Found formatted numbers like "5 596" or "- 17 349"
             # Parse them
             for m in matches:
                 candidates.append(_parse_number(m))
        else:
             # General capture again, but safer parsing
             # Capture isolated numbers
             # Update p2 to be robust too
             p2 = r"(?<![\d.])(?:\-?\s*)?\d+(?:,\d+)?(?!\d)" 
             matches2 = re.findall(p2, line)
             for m in matches2:
                 candidates.append(_parse_number(m))
    else:
        # We have distinct parts. Parse each part.
        for part in parts:
            val = _parse_number(part)
            if val is not None:
                candidates.append(val)
                
    if not candidates:
        return None
        
    valid = [x for x in candidates if x is not None]
    if not valid:
        return None
        
    # Heuristic: Return the LAST number
    return valid[-1]

def _find_metric_value(full_text: str, patterns: List[str]) -> Optional[float]:
    lines = full_text.splitlines()
    regexes = [re.compile(pat, re.IGNORECASE) for pat in patterns]

    best_val = None
    best_line = None
    max_numbers_in_line = 0

    for line in lines:
        for r in regexes:
            if r.search(line):
                # Count how many numbers look like they are in this line
                # "Total actif 5 596 5 674 6 033" -> 3 numbers
                # "6. Tresorerie" -> 1 number
                
                # Check for "percentage" signs. If line has %, ignore it for absolute value metrics?
                # Most of our metrics are currency. 
                # Exception: "dividende_par_action" might be small. 
                # But typically we want to avoid "Variation +5.3%".
                if "%" in line:
                    continue

                # Parse the line
                # We need a robust parser here.
                # Let's try the regex that identifies "space separated thousands" specifically
                # This matches "5 596" but NOT "5" or "596" (unless decimals)
                formatted_num_pat = r"\d{1,3}(?:[\s\u00a0]\d{3})+(?:,\d+)?"
                formatted_matches = re.findall(formatted_num_pat, line)
                
                count = len(formatted_matches)
                val = _find_value_in_line(line)
                
                if val is not None:
                    # Prefer lines with multiple formatted numbers (columns)
                    if count > max_numbers_in_line:
                        max_numbers_in_line = count
                        best_val = val
                        best_line = line
                    elif count == max_numbers_in_line and best_val is None:
                         best_val = val
                         best_line = line
                         
    return best_val

def extract_metrics_from_text(text: str) -> Dict[str, Optional[float]]:
    results = {}
    for metric, patterns in METRICS_MAPPING.items():
        results[metric] = _find_metric_value(text, patterns)
    return results

def process_pdfs():
    if not AMMC_RESULTS_DIR.exists():
        print(f"Directory {AMMC_RESULTS_DIR} does not exist. Please check the path.")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)
    
    all_records = []
    
    # Sort files to ensure order
    files = sorted(list(AMMC_RESULTS_DIR.glob("*.pdf")))
    
    for pdf_path in files:
        print(f"Processing {pdf_path.name}...")
        try:
            # Metadata
            ticker, year, trimestre = get_metadata_from_filename(pdf_path.name)
            
            # Extract text
            full_text = ""
            with pdfplumber.open(pdf_path) as pdf:
                # Limit pages? usually reports are long, but financial summary is often in first 10 or last 10 pages.
                # For safety, read all.
                for page in pdf.pages:
                    extract = page.extract_text(x_tolerance=2, y_tolerance=2)
                    if extract:
                        full_text += extract + "\n"
            
            # Extract metrics
            metrics = extract_metrics_from_text(full_text)
            
            record = {
                "ticker": ticker,
                "annee": year,
                "trimestre": trimestre,
                "norme_comptable": _norme_comptable_from_year(year),
                **metrics
            }
            all_records.append(record)
            
        except Exception as e:
            print(f"  Error extracting {pdf_path.name}: {e}")

    if not all_records:
        print("No records extracted.")
        return

    df = pd.DataFrame(all_records)
    
    # Save per year
    if 'annee' in df.columns:
        unique_years = df['annee'].unique()
        for year in sorted(unique_years):
            if year == 0: continue
            
            year_df = df[df['annee'] == year]
            year_df = year_df.sort_values(by="ticker")
            
            output_file = OUTPUT_DIR / f"extracted_{year}.csv"
            
            # Reorder columns to match requested format
            cols = ["ticker", "annee", "trimestre", "norme_comptable"] + list(METRICS_MAPPING.keys())
            # Ensure all columns exist
            for c in cols:
                if c not in year_df.columns:
                    year_df[c] = None
                    
            year_df.to_csv(output_file, index=False, columns=cols)
            print(f"Saved {len(year_df)} records for year {year} to {output_file}")
    else:
        print("Could not determine years for any records.")

if __name__ == "__main__":
    process_pdfs()
