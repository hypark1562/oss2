import logging
import os
import sys
from logging.handlers import RotatingFileHandler

# Force UTF-8 encoding for Windows compatibility (Must be at the top)
sys.stdout.reconfigure(encoding="utf-8")

# Import ETL Modules
from etl.extract import get_challenger_league
from etl.load import load_to_db
from etl.transform import process_data

# Ensure log directory exists
os.makedirs("logs", exist_ok=True)

# Centralized Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            "logs/etl.log",
            maxBytes=10 * 1024 * 1024,  # 10MB Limit
            backupCount=5,  # Keep last 5 files
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("ETL_Orchestrator")


def run_pipeline():
    """
    Execute the full ETL Pipeline.

    Strategy: Fail-Fast
    If any stage (Extract, Transform, Load) fails, the pipeline halts immediately
    to prevent data corruption or downstream errors.
    """
    logger.info("========== [ETL Pipeline] Started ==========")

    # Step 1: Extract
    if not get_challenger_league():
        logger.error("[ETL Pipeline] Halted: Extract phase failed.")
        return

    # Step 2: Transform
    if not process_data():
        logger.error("[ETL Pipeline] Halted: Transform phase failed.")
        return

    # Step 3: Load
    if not load_to_db():
        logger.error("[ETL Pipeline] Halted: Load phase failed.")
        return

    logger.info("========== [ETL Pipeline] Completed Successfully ==========")


if __name__ == "__main__":
    run_pipeline()
