import logging
import sys

# ETL 모듈 Import
from etl.extract import get_challenger_league
from etl.transform import process_data
from etl.load import load_to_db

# 전역 로깅 설정 (Console Output)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
        # logging.FileHandler("etl.log") # 필요하면 파일로도 저장 가능
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
