import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

def check_data():
    # DB 연결
    db_url = os.getenv("DB_URL") # sqlite:///lol_data.db
    engine = create_engine(db_url)
    
    try:
        # SQL 쿼리로 데이터 5줄만 가져오기
        df = pd.read_sql("SELECT * FROM matches LIMIT 5", con=engine)
        
        print("\n📊 [DB 데이터 확인 (상위 5개)]")
        print(df)
        print("\n✅ 데이터가 정상적으로 저장되어 있습니다!")
        
    except Exception as e:
        print(f"\n❌ 데이터를 읽을 수 없습니다. 테이블이 없는 것 같아요.\n에러: {e}")

if __name__ == "__main__":
    check_data()