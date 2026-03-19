import os
import re
import pandas as pd
import pdfplumber
from pathlib import Path
from typing import Dict, List, Optional

# Test single PDF extraction
PDF_PATH = Path("analysetest/scrapers/AMMC_Resultats/TGCC-2024.pdf")

# The IMPROVED metrics mapping from exctractionrapport.py
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
    name = filename.replace(".pdf", "")
    parts = name.split("-")
    ticker = parts[0]
    year_str = parts[-1]
    
    if not year_str.isdigit():
        match = re.search(r"(20\d{2})", name)
        year = int(match.group(1)) if match else 0
    else:
        year = int(year_str)
    
    trimestre = "S1" if "s1" in filename.lower() else "Annuel"
    return ticker, year, trimestre

def _norme_comptable_from_year(year: int) -> str:
    return "Marocaine" if year < 2018 else "IFRS"

def _parse_number(text: str) -> Optional[float]:
    if not text: return None
    clean_text = text.replace(" ", "").replace("\u00a0", "")
    if clean_text.startswith("(") and clean_text.endswith(")"):
        clean_text = "-" + clean_text[1:-1]
    clean_text = clean_text.replace(",", ".")
    clean_text = re.sub(r"[^\d.-]", "", clean_text)
    try:
        return float(clean_text)
    except ValueError:
        return None

def _find_value_in_line(line: str) -> Optional[float]:
    # 1. Skip lines that look like headers or text descriptions with percentages
    # If the line contains typical text-heavy indicators and few numbers, skip? 
    # Actually, relying on number extraction is safer.
    
    # Improved regex to capture "space-separated thousands" correctly.
    # We want to capture distinct numeric tokens.
    # Pattern: 
    #   Negative: - 12 345,67 or (12 345,67)
    #   Positive: 12 345,67
    
    # Breaking it down:
    # (?<![\d,]) : Lookbehind to ensure we don't start in middle of something (not perfect but helps)
    # (
    #   (?:-?\s*|\(?\s*)       # Optional sign or open paren
    #   \d{1,3}                # 1-3 digits
    #   (?:[\s\u00a0]\d{3})*   # Groups of 3 digits separated by space
    #   (?:,\d+)?              # Optional decimal
    #   \)?                    # Optional close paren
    # )
    
    # Note: We need to avoid capturing "2018" as a value if it's a year header, 
    # but often headers are "31-déc-18", so checking for letters helps.
    
    # This regex is stricter about what constitutes a number with spaces
    # It requires spaces to be followed by exactly 3 digits.
    regex = r"(?:^|(?<=\s)|(?<=^-\s))(?:\(?\-?\s*)?\d{1,3}(?:[\s\u00a0]\d{3})*(?:,\d+)?(?:\)?)(?=\s|$)"
    
    # Let's try a slightly looser one that captures the tokens we saw: "5 596"
    # The previous issue was "5 596 5 674" becoming one match? No, re.findall should split them 
    # if the pattern doesn't consume the separator space aggressively.
    
    # Specific pattern for the Morocco format "1 234 567"
    # We match "digits followed by (space + 3 digits) repeated"
    # We assume at least 2 spaces between columns if they are separate numbers? 
    # Or just one space? In "5 596 5 674", there is 1 space inside, 1 space between.
    # This is the ambiguity: "6 5 674" -> is it "65 674" or "6" and "5 674"?
    # Context helps: usually 3 columns.
    
    # HEURISTIC: Split by "  " (double space) first? 
    # PDF text extraction often preserves visual gaps as multiple spaces.
    
    # Let's try splitting by double space first.
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
        
    # Filter out likely years (e.g. 2017, 2018, 2019) if they appear in sequence?
    # Actually, years are usually in headers. In data rows, 2019 is a valid value.
    
    # Filter out None
    valid = [x for x in candidates if x is not None]
    
    if not valid:
        return None
        
    # Heuristic: Return the LAST number
    return valid[-1]

def _find_metric_value(full_text: str, patterns: List[str]) -> tuple[Optional[float], Optional[str]]:
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
                
                # Use a specific regex just to count potential columns
                # Matches "1 234" or "1234" or "12,34"
                # Exclude simple "1" or "2" digits if possible to avoid counting words? No, hard.
                
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
                         
    return best_val, best_line

def test_iam_2019_new_logic():
    print(f"Testing IMPROVED logic on: {PDF_PATH}")
    print("=" * 60)
    
    if not PDF_PATH.exists():
        print(f"ERROR: File not found at {PDF_PATH}")
        return
    
    # 1. Extract Text
    print("Extracting text...")
    full_text = ""
    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            # Using the improved tolerance settings
            extract = page.extract_text(x_tolerance=2, y_tolerance=2)
            if extract:
                full_text += extract + "\n"
    
    print(f"Extracted {len(full_text)} chars.")
    print("-" * 60)
    
    # 2. Test specific known values from screenshot first
    # Screenshot showed: "Trésorerie nette ... -13 042  -13 872  -17 349"
    # Expected for 2019 (last col): -17349
    
    print("Searching for metrics...")
    results = {}
    
    for metric, patterns in METRICS_MAPPING.items():
        val, line = _find_metric_value(full_text, patterns)
        results[metric] = val
        
        status = "✓ FOUND" if val is not None else "✗ MISSING"
        print(f"{status:10} {metric:30} : {val}")
        if val is not None:
             # Limit line length for display
             display_line = (line[:75] + '..') if len(line) > 75 else line
             print(f"           Source: \"{display_line}\"")
    
    print("-" * 60)
    
    # Check specifically for the Tresorerie nette issue
    tn_val = results.get("tresorerie_nette")
    print(f"Trésorerie nette check: {tn_val}")
    if tn_val == -17349.0:
        print("SUCCESS: Matches the screenshot value (-17 349) exactly!")
    elif tn_val == 17349.0:
        print("PARTIAL: Value matches but sign is wrong (should be negative).")
    else:
        print(f"WARNING: Value {tn_val} does not match expected -17349.")

if __name__ == "__main__":
    test_iam_2019_new_logic()
