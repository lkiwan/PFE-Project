from sqlalchemy import create_engine, inspect
import sys

# Connection string from agent1_analyst.py
DB_URL = "postgresql://postgres:123456@localhost:5432/PFE"

def check_db():
    try:
        engine = create_engine(DB_URL)
        inspector = inspect(engine)
        
        # Check if table exists
        if inspector.has_table("predictions", schema="md"):
            print("Table md.predictions exists.")
            # detailed column info
            columns = inspector.get_columns("predictions", schema="md")
            for col in columns:
                print(f"  - {col['name']} ({col['type']})")
        else:
            print("Table md.predictions DOES NOT exist.")
            
    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    check_db()
