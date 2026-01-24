"""
Module: load.py
Description: Persists transformed data to PostgreSQL using Incremental Load (Upsert).
             Maintains historical data integrity and prevents duplicate entries.
"""

import logging
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

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
    Executes Incremental Load via staging table to ensure atomic Upsert operations.
    Note: Requires UNIQUE constraint on 'summoner_id' in target table.
    """
    if df.empty:
        logger.info("[Load] No data to persist. Skipping task.")
        return

    engine = _get_engine()
    staging_table = "temp_challenger_stats"
    target_table = "challenger_stats"

    try:
        with engine.begin() as conn:
            # 1. Create temporary staging table
            df.to_sql(staging_table, con=conn, if_exists="replace", index=False)

            # 2. Execute Upsert (ON CONFLICT) logic
            # This ensures time-series data accumulation without primary key violations.
            upsert_query = text(
                f"""
                INSERT INTO {target_table} (player_name, summoner_id, lp, wins, losses, total_games, win_rate, updated_at)
                SELECT player_name, summoner_id, lp, wins, losses, total_games, win_rate, NOW()
                FROM {staging_table}
                ON CONFLICT (summoner_id)
                DO UPDATE SET
                    lp = EXCLUDED.lp,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    total_games = EXCLUDED.total_games,
                    win_rate = EXCLUDED.win_rate,
                    updated_at = NOW();
            """
            )

            conn.execute(upsert_query)

            # 3. Drop staging table
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

        logger.info(
            f"[Load] Incremental load completed successfully for {len(df)} records."
        )

    except SQLAlchemyError as e:
        logger.error(f"[Load] Database transaction failed: {str(e)}")
        raise
    finally:
        engine.dispose()
