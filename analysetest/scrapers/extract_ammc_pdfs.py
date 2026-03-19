import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

try:
    import pdfplumber  # type: ignore
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "Missing dependency 'pdfplumber'. Install it with:\n\n"
        "    pip install pdfplumber\n"
    ) from exc


AMMC_RESULTS_DIR = Path(__file__).parent / "AMMC_Resultats"


def _infer_ticker_from_name(name: str) -> str:
    """
    Infer ticker from file name.
    Keeps it simple and uses the first all‑caps token of length 2–5.
    """
    base = Path(name).stem
    # Normalise separators
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", base)
    candidates = [
        token
        for token in cleaned.split()
        if token.isupper() and 2 <= len(token) <= 5
    ]
    return candidates[0] if candidates else base[:5].upper()


def _infer_year(name: str, text: str) -> Optional[int]:
    """
    Infer financial year from file name then from PDF text.
    """
    # 1) Try filename
    m = re.search(r"(20[0-4][0-9])", name)
    if m:
        return int(m.group(1))

    # 2) Try header like "RAPPORT FINANCIER 2024"
    m = re.search(r"20[0-4][0-9]", text[:2000])
    if m:
        return int(m.group(0))

    return None


def _infer_trimestre(name: str) -> str:
    """
    Very simple heuristic based on filename.
    Fallback to 'FY' (full year).
    """
    lowered = name.lower()
    if any(k in lowered for k in ["s1", "semestriel", "semestre 1", "30 juin"]):
        return "S1"
    if any(k in lowered for k in ["s2", "semestre 2"]):
        return "S2"
    if any(k in lowered for k in ["mars", "q1"]):
        return "Q1"
    if any(k in lowered for k in ["septembre", "q3"]):
        return "Q3"
    if any(k in lowered for k in ["juin", "q2"]):
        return "Q2"
    if any(k in lowered for k in ["decembre", "décembre", "annuel", "rapports financiers 20"]):
        return "FY"
    return "FY"


def _norme_comptable_from_year(year: Optional[int]) -> Optional[str]:
    """
    Marocaine for years strictly before 2018, IFRS from 2018 onward.
    """
    if year is None:
        return None
    return "Marocaine" if year < 2018 else "IFRS"


def _extract_last_number(row: str) -> Optional[float]:
    """
    Given a line like:
        'Total actif 65 530 65 543 70 374'
    return the last numeric token (e.g. 70374.0).
    """
    # Accept numbers with spaces and commas
    nums = re.findall(r"\d[\d\s.,]*", row)
    if not nums:
        return None
    raw = nums[-1].replace(" ", "").replace("\u00a0", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _find_value(text: str, patterns: List[str]) -> Optional[float]:
    """
    Scan the text line by line and return the last number on the first
    line that matches any of the patterns.
    """
    lines = text.splitlines()
    regexes = [re.compile(pat, re.IGNORECASE) for pat in patterns]

    for line in lines:
        for r in regexes:
            if r.search(line):
                val = _extract_last_number(line)
                if val is not None:
                    return val
    return None


def extract_metrics_from_text(text: str) -> Dict[str, Optional[float]]:
    """
    Extract the key metrics needed for your unified dataset.
    Many items will be None when not found – you can back‑fill
    manually later if needed.
    """
    return {
        # Balance sheet
        "total_actif": _find_value(text, [r"\btotal\s+actif\b"]),
        "capitaux_propres_pg": _find_value(
            text,
            [
                r"capitaux\s+propres\s*[-–]\s*part\s+du\s+groupe",
                r"capitaux\s+propres\s+pg",
            ],
        ),
        "dettes_financieres_lt": _find_value(
            text,
            [
                r"dette[s]?\s+financi[eè]re[s]?\s+non\s+courante[s]?",
                r"dette[s]?\s+financi[eè]re[s]?\s+long\s+terme",
            ],
        ),
        "dettes_financieres_ct": _find_value(
            text,
            [
                r"dette[s]?\s+financi[eè]re[s]?\s+courante[s]?",
                r"dette[s]?\s+financi[eè]re[s]?\s+court\s+terme",
            ],
        ),
        "tresorerie_nette": _find_value(
            text,
            [
                r"tr[eé]sorerie\s+nette",
                r"position\s+de\s+tr[eé]sorerie\s+nette",
            ],
        ),
        "creances_clients": _find_value(
            text,
            [
                r"cr[eé]ances\s+clients?",
                r"cr[eé]ances\s+client[eè]le",
            ],
        ),
        # Income statement
        "chiffre_d_affaires_pnb": _find_value(
            text,
            [
                r"chiffre\s+d['’]affaires",
                r"produit\s+net\s+bancaire",
                r"\bPNB\b",
            ],
        ),
        "resultat_brut_exploitation": _find_value(
            text,
            [
                r"r[eé]sultat\s+brut\s+d['’]exploitation",
                r"rbe\b",
            ],
        ),
        "resultat_net_pg": _find_value(
            text,
            [
                r"r[eé]sultat\s+net\s+part\s+du\s+groupe",
                r"r[eé]sultat\s+net\s+pg",
            ],
        ),
        "cout_du_risque": _find_value(
            text,
            [
                r"co[uû]t\s+du\s+risque",
            ],
        ),
        "dotations_amortissements": _find_value(
            text,
            [
                r"dotations?\s+aux\s+amortissements",
                r"amortissements\s+et\s+provisions",
            ],
        ),
        # Cash‑flow & market
        "flux_tresorerie_operationnel": _find_value(
            text,
            [
                r"flux\s+de\s+tr[eé]sorerie\s+li[eé]\s+aux\s+activit[eé]s?\s+op[eé]rationnelles",
                r"flux\s+de\s+tr[eé]sorerie\s+op[eé]rationnel",
            ],
        ),
        "capex": _find_value(
            text,
            [
                r"capex",
                r"d[eé]penses?\s+d['’]investissement",
                r"investissements?\s+corporels",
            ],
        ),
        "nombre_actions_circulation": _find_value(
            text,
            [
                r"nombre\s+d['’]actions\s+en\s+circulation",
                r"nombre\s+total\s+d['’]actions",
            ],
        ),
        "dividende_par_action": _find_value(
            text,
            [
                r"dividende\s+par\s+action",
                r"DPA\b",
            ],
        ),
        # 'prix_cloture' is typically a market‑data field; we do not
        # expect to find it in the PDF, so default to None.
        "prix_cloture": None,
    }


def extract_pdf(path: Path) -> Dict[str, object]:
    """
    Extract one record (one line) from a single AMMC PDF.
    """
    with pdfplumber.open(path) as pdf:
        text_parts: List[str] = []
        for page in pdf.pages:
            try:
                text_parts.append(page.extract_text() or "")
            except Exception:
                continue
        full_text = "\n".join(text_parts)

    ticker = _infer_ticker_from_name(path.name)
    year = _infer_year(path.name, full_text)
    trimestre = _infer_trimestre(path.name)
    norme = _norme_comptable_from_year(year)

    metrics = extract_metrics_from_text(full_text)

    record: Dict[str, object] = {
        "ticker": ticker,
        "annee": year,
        "trimestre": trimestre,
        "norme_comptable": norme,
    }
    record.update(metrics)
    return record


def run_scan(output_csv: Optional[Path] = None) -> pd.DataFrame:
    """
    Scan all PDFs in `AMMC_Resultats` and return a DataFrame with the
    exact columns you described.
    """
    if not AMMC_RESULTS_DIR.exists():
        raise FileNotFoundError(f"Folder not found: {AMMC_RESULTS_DIR}")

    records: List[Dict[str, object]] = []
    for pdf_path in sorted(AMMC_RESULTS_DIR.glob("*.pdf")):
        print(f"Processing {pdf_path.name} ...")
        try:
            records.append(extract_pdf(pdf_path))
        except Exception as exc:
            print(f"  !! Error on {pdf_path.name}: {exc}")

    cols = [
        # 1) Métadonnées
        "ticker",
        "annee",
        "trimestre",
        "norme_comptable",
        # 2) Bilan
        "total_actif",
        "capitaux_propres_pg",
        "dettes_financieres_lt",
        "dettes_financieres_ct",
        "tresorerie_nette",
        "creances_clients",
        # 3) Compte de résultat
        "chiffre_d_affaires_pnb",
        "resultat_brut_exploitation",
        "resultat_net_pg",
        "cout_du_risque",
        "dotations_amortissements",
        # 4) Flux & marché
        "flux_tresorerie_operationnel",
        "capex",
        "nombre_actions_circulation",
        "dividende_par_action",
        "prix_cloture",
    ]

    df = pd.DataFrame(records, columns=cols)

    if output_csv is not None:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"\nSaved {len(df)} rows to {output_csv}")

    return df


if __name__ == "__main__":
    # Default: write to analysetest/AMMC_extracted.csv
    default_out = (
        Path(__file__).resolve().parents[1] / "AMMC_extracted.csv"
    )
    run_scan(default_out)

