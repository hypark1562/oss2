import pandas as pd
from sqlalchemy import create_engine
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


def load_to_db() -> bool:
    """
    전처리된 데이터를 읽어 Data Warehouse(SQLite/MySQL)에 적재합니다.

    Schema Info:
        - Table 'users': 소환사 정보를 담고 있는 Dimension Table.
        - Strategy: 'replace' (배치 수행 시마다 갱신). 추후 Incremental Update로 고도화 예정.

    Returns:
        bool: 적재 성공 여부
    """
    try:
        # 1. DB Connection 생성
        db_url = os.getenv("DB_URL")
        if not db_url:
            logger.error("❌ [Load] DB_URL 환경변수가 없습니다.")
            return False

        engine = create_engine(db_url)

        # 2. Processed Data 로드
        file_path = "data/processed/cleaned_data.csv"
        if not os.path.exists(file_path):
            logger.error(f"❌ [Load] 전처리된 파일이 없습니다: {file_path}")
            return False

        df = pd.read_csv(file_path)

        # 3. 데이터 적재 (Loading)
        # 현재는 유저 정보이므로 'users' 테이블에 적재 (ERD 설계 준수)
        table_name = "users"
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)

        logger.info(f"✅ [Load] DB 적재 완료! (Table: {table_name}, Rows: {len(df)})")
        return True

    except Exception as e:
        logger.exception(f"❌ [Load] DB 적재 중 오류 발생: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_to_db()
