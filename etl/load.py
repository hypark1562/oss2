"""
Module: load.py
Persists transformed data to PostgreSQL.
Engine: SQLAlchemy for connection pooling and ORM compatibility.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

def _get_engine():
    user = os.getenv("DB_USER", "admin")
    pw = os.getenv("DB_PASSWORD", "password123")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "lol_analytics")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")


def load_data(df):
    """
    Bulk load DataFrame to PostgreSQL.
    Configured with 'replace' mode for initial build phase.
    """
    if df.empty:
        logger.info("No data to load. Task skipped.")
        return

    engine = _get_engine()
    
    try:
        # Bulk insert with 1000-row chunks for memory efficiency
        df.to_sql(
            "challenger_stats",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi"
        )
        logger.info("Database load successful.")
        
    except SQLAlchemyError as e:
        logger.error(f"DB Load Failure: {e}")
        raise
    finally:
        engine.dispose() # Clean up connection pool