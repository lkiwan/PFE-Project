import pandas as pd
from datetime import datetime
from db.database import SessionLocal
from typing import List, Dict

class BaseScraper:
    def __init__(self):
        self.session = SessionLocal()

    def fetch_data(self) -> pd.DataFrame:
        """
        To be implemented by subclasses.
        Should return a Pandas DataFrame containing the scraped data.
        """
        raise NotImplementedError("Subclasses must implement fetch_data")

    def save_to_db(self, df: pd.DataFrame):
        """
        To be implemented by subclasses.
        Should map the DataFrame to SQLAlchemy models and save to DB.
        """
        raise NotImplementedError("Subclasses must implement save_to_db")

    def run(self):
        """
        Orchestrates the scraping and ingestion process.
        """
        df = self.fetch_data()
        self.save_to_db(df)
        print(f"[{datetime.now()}] Ingestion completed for {self.__class__.__name__}.")
