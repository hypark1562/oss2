import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def load_to_db():
     
    db_url = os.getenv("DB_URL") # sqlite:///lol_data.db
    engine = create_engine(db_url)
    
    
    df = pd.read_csv("data/processed/cleaned_data.csv")
    
    
    df.to_sql("users", con=engine, if_exists='replace', index=False)
    print("✅ DB 적재 완료! (Table: users)")

if __name__ == "__main__":
    load_to_db()