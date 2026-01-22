"""
Module: main.py
Description: Entry point for LoL Data Pipeline.
             Orchestrates ETL phases and manages global exception handling.
"""

import sys
import logging
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from utils.alert import send_slack_alert
from utils.logger import setup_logger

logger = setup_logger()


def run_pipeline():
    """
    Execute full ETL workflow: Extract -> Transform -> Load.
    Integrated with Slack webhook for real-time monitoring.
    """
    try:
        # Initialize process and notify start
        logger.info("Initializing ETL Pipeline...")
        send_slack_alert("ETL Process Started", level="INFO")

        # Phase 1: Data Acquisition
        # Note: Depends on Riot API availability and Rate Limit quota.
        raw_data = extract_data()
        
        # Phase 2: Business Logic & Feature Engineering
        # Process raw JSON into structured DataFrame for analytics.
        clean_df = transform_data(raw_data)

        # Phase 3: Data Persistence
        # Target: PostgreSQL (Production instance via Docker)
        load_data(clean_df)

        # Finalize and report success metrics
        success_log = f"Pipeline execution completed. Records: {len(clean_df)}"
        logger.info(success_log)
        send_slack_alert(success_log, level="INFO")

    except Exception as e:
        # Log critical failure context for incident response
        logger.error(f"Execution Error: {str(e)}", exc_info=True)
        send_slack_alert(f"Pipeline Failure: {type(e).__name__}", level="ERROR")
        sys.exit(1)

if __name__ == "__main__":
    print("!!! Pipeline execution started directly !!!") # 이 글자가 뜨는지 확인
    run_pipeline()