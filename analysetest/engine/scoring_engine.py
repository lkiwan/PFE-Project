import pandas as pd
from db.database import engine

def load_data() -> dict:
    """Loads tables from SQL and returns a dictionary of DataFrames."""
    dfs = {}
    
    with engine.connect() as conn:
        dfs['prix'] = pd.read_sql("SELECT * FROM prix_historiques", conn)
        dfs['finances'] = pd.read_sql("SELECT * FROM etats_financiers", conn)
        dfs['macro'] = pd.read_sql("SELECT * FROM macroeconomie", conn)
        
    return dfs

def calculate_ratios(dfs: dict) -> pd.DataFrame:
    """Calculates financial ratios (PER, ROE, Debt/Equity) from combined data."""
    # Assuming the most recent financial data
    df_fin = dfs['finances'].copy()
    
    # Calculate simple ROE (Return on Equity) = Net Income / Equity
    df_fin['roe'] = df_fin['resultat_net'] / df_fin['capitaux_propres']
    
    # Calculate Debt/Equity
    df_fin['debt_to_equity'] = (df_fin['dette_court_terme'] + df_fin['dette_long_terme']) / df_fin['capitaux_propres']
    
    # Get latest market cap for PER calculation
    # Sort prices by date descending and get the most recent row per symbol
    df_prix = dfs['prix'].sort_values('date', ascending=False).drop_duplicates('symbole')
    
    # Merge financial and latest price stats
    df_merged = pd.merge(df_fin, df_prix[['symbole', 'capitalisation_boursiere', 'prix_cloture', 'date']], on='symbole', how='inner')
    
    # Calculate PER (Price Earnings Ratio) = Market Cap / Net Income
    df_merged['per'] = df_merged['capitalisation_boursiere'] / df_merged['resultat_net']
    
    return df_merged

def apply_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """Applies point-based scoring rules."""
    df['score'] = 0
    
    # Rule 1: ROE > 15% (+3 points)
    df.loc[df['roe'] > 0.15, 'score'] += 3
    
    # Rule 2: Debt to Equity < 1 (+3 points)
    df.loc[df['debt_to_equity'] < 1.0, 'score'] += 3
    
    # Rule 3: PER between 10 and 20 (Value Play) (+2 points)
    df.loc[(df['per'] >= 10) & (df['per'] <= 20), 'score'] += 2
    
    # Rule 4: Positive Free Cash Flow (+2 points)
    df.loc[df['free_cash_flow'] > 0, 'score'] += 2
    
    return df

def run_scoring():
    print("Chargement des données depuis la base de données...")
    dfs = load_data()
    
    if dfs['finances'].empty or dfs['prix'].empty:
        print("Aucune donnée disponible pour le scoring. Veuillez d'abord extraire les données.")
        return
        
    print("Calcul des ratios financiers...")
    df_ratios = calculate_ratios(dfs)
    
    print("Application du moteur de règles et calcul du score sur 10...")
    df_scored = apply_scoring(df_ratios)
    
    print("\nRésultats du Scoring (Top Actions) :")
    results = df_scored[['symbole', 'per', 'roe', 'debt_to_equity', 'score']].sort_values('score', ascending=False)
    print(results.to_string(index=False))
    
    # Export to CSV for Power BI visualization
    output_path = 'data/resultats_scoring.csv'
    df_scored.to_csv(output_path, index=False)
    print(f"\nDonnées exportées pour Power BI vers : {output_path}")

if __name__ == "__main__":
    run_scoring()
