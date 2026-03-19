import os
import re

folder = r"c:\Users\arhou\OneDrive\Bureau\PFE project\analysetest\scrapers\AMMC_Resultats"
files = os.listdir(folder)

def normalize_company(name):
    name = name.lower().replace('-', '_').replace(' ', '_').replace('%20', '_')
    if 'addoha' in name: return 'ADDOHA'
    if 'adi' in name: return 'ADI'
    if 'atw' in name: return 'ATW'
    if 'awb' in name: return 'AWB'
    if 'akdital' in name: return 'AKDITAL'
    if 'cih' in name: return 'CIH'
    if 'dpga' in name: return 'DPGA'
    if 'iam' in name or 'maroc_telecom' in name or 'maroc_teecom' in name: return 'IAM'
    if 'jet_contractors' in name: return 'JET_CONTRACTORS'
    if 'marsa' in name: return 'MARSA_MAROC'
    if 'tgcc' in name: return 'TGCC'
    if 'taqa' in name: return 'TAQA_MOROCCO'
    return "UNKNOWN"

def get_year(name):
    name = name.lower()
    match = re.search(r'(20\d{2})', name)
    if match: return match.group(1)
    match = re.search(r'_(18|19|20|21|22|23|24|25)', name)
    if match: return "20" + match.group(1)
    return "UNKNOWN"

def is_rfa(name):
    return 'rfa' in name.lower() or 'annuel' in name.lower() or 'conso' in name.lower()

mapping = {}
for f in files:
    if not f.endswith('.pdf'): continue
    comp = normalize_company(f)
    year = get_year(f)
    if comp == "UNKNOWN" or year == "UNKNOWN":
        print(f"Cannot parse {f}")
        continue
    
    key = f"{comp} - {year}.pdf"
    path = os.path.join(folder, f)
    size = os.path.getsize(path)
    rfa = is_rfa(f)
    
    if key not in mapping:
        mapping[key] = (f, rfa, size)
    else:
        # compare
        existing_f, existing_rfa, existing_size = mapping[key]
        if rfa and not existing_rfa:
            mapping[key] = (f, rfa, size)
            print(f"Replacing {existing_f} with {f} for {key} (RFA preferred)")
            os.remove(os.path.join(folder, existing_f))
        elif not rfa and existing_rfa:
            print(f"Skipping {f} for {key} (RFA already exists)")
            os.remove(path)
        else:
            if size > existing_size:
                mapping[key] = (f, rfa, size)
                print(f"Replacing {existing_f} with {f} for {key} (Size preferred)")
                os.remove(os.path.join(folder, existing_f))
            else:
                print(f"Skipping {f} for {key} (Size smaller)")
                os.remove(path)

# Finally rename the remaining files
for key, (old_f, _, _) in mapping.items():
    if old_f != key:
        old_path = os.path.join(folder, old_f)
        new_path = os.path.join(folder, key)
        if os.path.exists(new_path) and old_f != key:
            # edge case if target name already exists from previous runs/different cases
            # shouldn't happen based on the dict logic unless original name was completely same
            pass
        print(f"Renaming {old_f} -> {key}")
        os.rename(old_path, new_path)

print("Renaming and cleanup complete!")
