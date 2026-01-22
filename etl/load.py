import logging
import os
import sqlite3

import pandas as pd

from utils.config import config

logger = logging.getLogger(__name__)


def load_to_db() -> bool:
    """
    Load Processed Data (CSV) into SQLite Database.

    This function ensures idempotency by using the 'replace' strategy.
    The table is dropped and recreated on every run to maintain a fresh state.

    Returns:
        bool: True if loading is successful, False otherwise.
    """
    try:
        csv_path = config["path"]["processed_data"]
        db_path = config["path"]["db_path"]

        if not os.path.exists(csv_path):
            logger.error(f"[Load] Source CSV file not found: {csv_path}")
            return False

        # Load Data
        df = pd.read_csv(csv_path)

        # Connect to Database (Creates file if not exists)
        conn = sqlite3.connect(db_path)

        # Write to Table
        table_name = "users"
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        conn.close()

        logger.info(
            f"[Load] Successfully loaded {len(df)} records into '{table_name}' table."
        )
        return True

    except Exception as e:
        logger.exception(f"[Load] Database Operation Error: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_to_db()
