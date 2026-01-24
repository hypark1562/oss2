"""
Module: load.py
Description: Persists transformed data to the target database using Upsert logic.
             Supports both SQLite (Local) and PostgreSQL (Production) environments
             with robust connection handling and timeout management.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Initialize logger for data persistence monitoring
logger = logging.getLogger(__name__)


def _get_engine():
    """
    Constructs the SQLAlchemy engine based on the environment-specific DB_URL.
    Includes SQLite-specific arguments to prevent 'Database is locked' errors.
    """
    db_url = os.getenv("DB_URL", "sqlite:///lol_data.db")

    # SQLite requires specific threading and timeout arguments for concurrent access
    if "sqlite" in db_url:
        return create_engine(
            db_url, connect_args={"timeout": 30, "check_same_thread": False}
        )

    # PostgreSQL standard engine initialization
    return create_engine(db_url)


def load_data(df: pd.DataFrame):
    """
    Executes data persistence via 'Replace' or 'Upsert' strategy.
    Note: SQLite uses 'replace' for simplicity, while PostgreSQL utilizes staging tables.
    """
    if df.empty:
        logger.info("[Load] No data to persist. Skipping task.")
        return

    engine = _get_engine()
    db_url = os.getenv("DB_URL", "")
    target_table = "challenger_stats"

    try:
        # 1. Handling SQLite (Local/Testing Environment)
        # SQLite does not support advanced PostgreSQL Upsert syntax (ON CONFLICT)
        if "sqlite" in db_url or not db_url:
            with engine.begin() as conn:
                df.to_sql(target_table, con=conn, if_exists="replace", index=False)
            logger.info(f"[Load] SQLite full-replace completed for {len(df)} records.")

        # 2. Handling PostgreSQL (Production Environment)
        # Implements atomic Upsert logic via temporary staging table
        else:
            staging_table = "temp_challenger_stats"
            with engine.begin() as conn:
                # Create staging table
                df.to_sql(staging_table, con=conn, if_exists="replace", index=False)

                # Execute Upsert logic for PostgreSQL
                upsert_query = text(
                    f"""
                    INSERT INTO {target_table} (player_name, summoner_id, lp, wins, losses, total_games, win_rate, updated_at)
                    SELECT player_name, summoner_id, lp, wins, losses, total_games, win_rate, CURRENT_TIMESTAMP
                    FROM {staging_table}
                    ON CONFLICT (summoner_id)
                    DO UPDATE SET
                        lp = EXCLUDED.lp,
                        wins = EXCLUDED.wins,
                        losses = EXCLUDED.losses,
                        total_games = EXCLUDED.total_games,
                        win_rate = EXCLUDED.win_rate,
                        updated_at = CURRENT_TIMESTAMP;
                """
                )
                conn.execute(upsert_query)
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

            logger.info(
                f"[Load] PostgreSQL incremental load completed for {len(df)} records."
            )

    except SQLAlchemyError as e:
        logger.error(
            f"[Load] Database transaction failed: {type(e).__name__} - {str(e)}"
        )
        raise
    finally:
        engine.dispose()
