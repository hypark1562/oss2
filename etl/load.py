import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def load_to_db():
    # 1. DB 연결 (메타코드 내용)
    db_url = os.getenv("DB_URL") # sqlite:///lol_data.db
    engine = create_engine(db_url)
    
    # 2. 데이터 읽기
    df = pd.read_csv("data/processed/cleaned_data.csv")
    
    # 3. DB에 넣기 (테이블 이름: matches)
    # if_exists='replace'는 덮어쓰기, 'append'는 추가하기
    df.to_sql("matches", con=engine, if_exists='replace', index=False)
    print("✅ DB 적재 완료! (Table: matches)")

if __name__ == "__main__":
    load_to_db()