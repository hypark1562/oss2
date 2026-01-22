import logging
import os

from dotenv import load_dotenv

# 기존 모듈들
from etl.extract import extract_data
from etl.load import load_data
from etl.transform import transform_data
# 우리가 방금 만든 알림 함수 가져오기
from utils.alert import send_slack_alert
from utils.logger import setup_logger

# 환경변수 로드 (.env 파일 읽기)
load_dotenv()

# 로거 설정
logger = setup_logger()


def main():
    try:
        # [시작 알림] 파이프라인 시작한다고 슬랙에 보고
        logger.info("ETL Pipeline Started...")
        send_slack_alert("ETL 파이프라인이 작업을 시작했습니다. 🏃‍♂️", level="INFO")

        # 1. Extract (데이터 수집)
        logger.info("Step 1: Extracting data from Riot API...")
        raw_data = extract_data()

        # 2. Transform (데이터 변환)
        logger.info("Step 2: Transforming data...")
        clean_df = transform_data(raw_data)

        # 3. Load (데이터 적재)
        logger.info("Step 3: Loading data into Database...")
        load_data(clean_df)

        # [성공 알림] 다 끝났으면 성공했다고 보고
        logger.info("ETL Pipeline Completed Successfully.")
        send_slack_alert(
            f"ETL 작업 성공! 총 {len(clean_df)}건의 데이터가 저장되었습니다. 🎉", level="INFO"
        )

    except Exception as e:
        # [실패 알림] 에러 나면 즉시 빨간색 알림 발송!
        logger.error(f"ETL Pipeline Failed: {e}")
        error_message = f"작업 중 심각한 에러가 발생했습니다.\n에러 내용: {str(e)}"
        send_slack_alert(error_message, level="ERROR")

        # 프로그램 비정상 종료 처리
        raise e


if __name__ == "__main__":
    main()
