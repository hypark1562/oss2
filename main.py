"""
Module: main
Description:
    Entry point for the LoL Data Pipeline.
    Orchestrates the ETL workflow (Extract -> Transform -> Load) and handles
    global exceptions and notifications.
"""
import logging
import sys

from etl.extract import extract_data
from etl.load import load_data
from etl.transform import transform_data
from utils.alert import send_slack_alert
from utils.logger import setup_logger

# Initialize global logger
logger = setup_logger()


def run_pipeline() -> None:
    """
    Executes the End-to-End ETL process.

    Flow:
        1. Extract: Fetch data from Riot API.
        2. Transform: Cleanse and engineer features.
        3. Load: Persist data to the target database.
        4. Notify: Send execution status to Slack.
    """
    try:
        logger.info(">>> Pipeline Execution Started")
        send_slack_alert("🚀 ETL Pipeline Started", level="INFO")

        # [Step 1] Extraction
        raw_data = extract_data()

        # [Step 2] Transformation
        clean_df = transform_data(raw_data)

        # [Step 3] Loading
        load_data(clean_df)

        # [Completion]
        success_msg = f"✅ Pipeline Succeeded. Processed {len(clean_df)} records."
        logger.info(success_msg)
        send_slack_alert(success_msg, level="INFO")

    except Exception as e:
        # [Error Handling] Catch-all for unexpected failures
        error_msg = f"❌ Pipeline Failed: {type(e).__name__} - {str(e)}"
        logger.critical(error_msg, exc_info=True)
        send_slack_alert(error_msg, level="ERROR")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
