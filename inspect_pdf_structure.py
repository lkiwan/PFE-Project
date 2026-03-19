import pdfplumber
import pandas as pd
from pathlib import Path

PDF_PATH = Path("analysetest/scrapers/AMMC_Resultats/IAM-2019.pdf")

def inspect_pdf_structure():
    if not PDF_PATH.exists():
        print(f"File not found: {PDF_PATH}")
        return

    print(f"Inspecting {PDF_PATH}...")
    
    with pdfplumber.open(PDF_PATH) as pdf:
        # We'll check a few pages, maybe around page 52 as seen in the screenshot? 
        # The screenshot shows page 52 of 76.
        pages_to_check = [51, 52, 53] # 0-indexed, so page 52 is index 51
        
        for i in pages_to_check:
            if i >= len(pdf.pages):
                continue
                
            page = pdf.pages[i]
            print(f"\n--- Page {i+1} ---")
            
            # 1. Try extracting tables
            tables = page.extract_tables()
            print(f"Found {len(tables)} tables.")
            
            for j, table in enumerate(tables):
                print(f"Table {j+1}:")
                df = pd.DataFrame(table)
                print(df.head()) # Print first few rows of the table
                print("..." if len(df) > 5 else "")
            
            # 2. Extract raw text to see layout if tables fail
            if not tables:
                print("No tables found. Raw text sample:")
                text = page.extract_text()
                lines = text.split('\n')
                # Print lines that look like the one in the screenshot
                for line in lines:
                    if "Trésorerie" in line or "31-déc" in line:
                        print(f"LINE: {line}")

if __name__ == "__main__":
    inspect_pdf_structure()
