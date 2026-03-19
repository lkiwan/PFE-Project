import pandas as pd
from datetime import date
from scrapers.base_scraper import BaseScraper
from db.models import Macroeconomie

class MacroDataScraper(BaseScraper):
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch macroeconomic data from Bank Al-Maghrib and HCP.
        This is a template. Implement actual scraping logic here.
        """
        columns = ['date', 'taux_directeur', 'taux_inflation', 'rendement_bons_tresor_10a']
        return pd.DataFrame(columns=columns)

    def save_to_db(self, df: pd.DataFrame):
        if df.empty:
            print("No macroeconomic records to save.")
            return

        for index, row in df.iterrows():
            record = Macroeconomie(
                date=row['date'],
                taux_directeur=row['taux_directeur'],
                taux_inflation=row['taux_inflation'],
                rendement_bons_tresor_10a=row['rendement_bons_tresor_10a']
            )
            self.session.add(record)
        
        self.session.commit()
        print(f"Saved {len(df)} macroeconomic records to database.")

if __name__ == "__main__":
    scraper = MacroDataScraper()
    scraper.run()
