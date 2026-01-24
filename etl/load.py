"""
Module: etl.load
Description:
    Manages data persistence to SQLite (Development) or PostgreSQL (Production).
    Implements Atomic Upsert strategy to ensure idempotency.
"""
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def _get_engine():
    """
    Constructs a SQLAlchemy engine with environment-specific configurations.

    Returns:
        sqlalchemy.engine.Engine: Configured database engine.
    """
    db_url = os.getenv("DB_URL", "sqlite:///lol_data.db")

    # [Performance] SQLite optimization for high concurrency
    if "sqlite" in db_url:
        return create_engine(
            db_url, connect_args={"timeout": 30, "check_same_thread": False}
        )
    return create_engine(db_url)


def load_data(df: pd.DataFrame) -> None:
    """
    Loads transformed data into the target database.

    Strategy:
        - SQLite: Full Replace (due to limited Upsert support).
        - PostgreSQL: Atomic Upsert (INSERT ON CONFLICT UPDATE) via staging table.

    Args:
        df (pd.DataFrame): Cleaned data to persist.
    """
    if df.empty:
        logger.info("[Load] No records to load. Skipping.")
        return

    engine = _get_engine()
    target_table = "challenger_stats"
    db_url = os.getenv("DB_URL", "")

    try:
        # 1. SQLite: Simple Replace Strategy (Dev/Test)
        if "sqlite" in db_url or not db_url:
            with engine.begin() as conn:
                df.to_sql(target_table, con=conn, if_exists="replace", index=False)
            logger.info(f"[Load] SQLite: Successfully loaded {len(df)} records.")

        # 2. PostgreSQL: Staging Table + Upsert Strategy (Production)
        else:
            staging_table = "temp_challenger_stats"
            with engine.begin() as conn:
                # Load to temporary staging table first
                df.to_sql(staging_table, con=conn, if_exists="replace", index=False)

                # Execute Upsert (Idempotent Operation)
                upsert_query = text(
                    f"""
                    INSERT INTO {target_table}
                        (player_name, summoner_id, lp, wins, losses, total_games, win_rate, updated_at)
                    SELECT
                        player_name, summoner_id, lp, wins, losses, total_games, win_rate, CURRENT_TIMESTAMP
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

            logger.info(f"[Load] PostgreSQL: Upsert completed for {len(df)} records.")

    except SQLAlchemyError as e:
        logger.critical(f"[Load] Database transaction failed: {e}")
        raise
    finally:
        engine.dispose()
