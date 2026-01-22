import logging
import os
import sqlite3

import pandas as pd

# 방금 만든 로거 가져오기
from utils.logger import setup_logger

logger = setup_logger()


def load_data(df: pd.DataFrame, db_name="data/lol_data.db"):
    """
    전처리된 데이터를 SQLite DB 파일에 저장합니다.
    """
    try:
        logger.info("[Load] Starting data loading to SQLite...")

        # 1. 데이터가 비어있으면 스킵
        if df.empty:
            logger.warning("[Load] DataFrame is empty. Skipping load.")
            return

        # 2. 저장할 폴더가 없으면 생성
        os.makedirs(os.path.dirname(db_name), exist_ok=True)

        # 3. SQLite DB 연결 (없으면 자동 생성됨)
        conn = sqlite3.connect(db_name)

        # 4. 데이터 저장 (테이블 이름: challenger_stats)
        # if_exists='replace': 기존 데이터 지우고 새로 씀
        df.to_sql("challenger_stats", conn, if_exists="replace", index=False)

        conn.close()
        logger.info(f"[Load] Successfully loaded {len(df)} rows into '{db_name}'.")

    except Exception as e:
        logger.error(f"[Load] Failed to load data: {e}")
        raise e
