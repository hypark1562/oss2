import logging
import os
import sys
from logging.handlers import RotatingFileHandler

# ETL 모듈 Import
from etl.extract import get_challenger_league
from etl.load import load_to_db
from etl.transform import process_data

os.makedirs("logs ", exist_ok=True)

# 전역 로깅 설정 (Console Output)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),  # 화면에도 출력하고
        RotatingFileHandler(
            "logs/etl.log ",
            maxBytes=10 * 1024 * 1024,  # 10MB 넘으면
            backupCount=5,  # 옛날 파일 5개까지만 보관하고 나머지 삭제
        ),
    ],
)
logger = logging.getLogger("ETL_Pipeline")


def run_pipeline():
    """
    [ETL Orchestrator]
    데이터 수집(Extract) -> 전처리(Transform) -> 적재(Load) 과정을 순차적으로 실행합니다.
    한 단계라도 실패 시 파이프라인을 즉시 중단(Fail-Fast)합니다.
    """
    logger.info("🚀 [ETL Pipeline] 작업을 시작합니다...")

    if not get_challenger_league():
        logger.error("🛑 Extract 단계 실패로 파이프라인 중단")
        return

    if not process_data():
        logger.error("🛑 Transform 단계 실패로 파이프라인 중단")
        return

    if not load_to_db():
        logger.error("🛑 Load 단계 실패로 파이프라인 중단")
        return

    logger.info("✨ [ETL Pipeline] 모든 작업이 성공적으로 완료되었습니다! ✨")


if __name__ == "__main__":
    run_pipeline()
