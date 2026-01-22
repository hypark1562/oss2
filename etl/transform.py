"""
Module: transform.py
Description: Implementation of data cleansing and feature engineering logic.
             Ensures schema consistency and calculates business KPIs (Win Rates).
"""

import logging
import pandas as pd
import numpy as np

# logger instance inheritance
logger = logging.getLogger(__name__)

def transform_data(raw_data):
    """
    Standardize raw API response into a structured DataFrame.
    Implements defensive logic for API schema changes.
    """
    try:
        # Step 1: Input Validation
        # Terminate process if upstream data is unavailable
        if not raw_data:
            logger.error("[Transform] Null payload received from extract phase.")
            raise ValueError("TRANSFORM_INPUT_EMPTY")

        df = pd.DataFrame(raw_data)

        # Step 2: Dynamic Schema Mapping
        # Define target mapping for database normalization
        # Note: Riot API fields are subject to change; implementing soft-filtering.
        schema = {
            "summonerName": "player_name",
            "summonerId": "summoner_id",
            "leaguePoints": "lp",
            "wins": "wins",
            "losses": "losses"
        }

        # Select only existing columns to prevent KeyError during API updates
        actual_columns = [col for col in schema.keys() if col in df.columns]
        
        # Mapping logic with exception-safe renaming
        df = df[actual_columns].rename(columns={k: v for k, v in schema.items() if k in actual_columns})

        # Step 3: Feature Engineering
        # Handling potential missing values in numeric fields to ensure calculation stability
        for col in ['wins', 'losses']:
            if col not in df.columns:
                df[col] = 0

        # Calculate Win Rate KPI
        # Note: Using np.where to handle ZeroDivision scenarios for low-volume match data
        df["total_games"] = df["wins"] + df["losses"]
        df["win_rate"] = np.where(
            df["total_games"] > 0,
            (df["wins"] / df["total_games"] * 100).round(2),
            0.0
        )

        # Step 4: Data Quality (Sorting and Indexing)
        # Sort by LP to maintain ranking hierarchy
        result_df = df.sort_values(by="lp", ascending=False).reset_index(drop=True)

        logger.info(f"[Transform] Successfully processed {len(result_df)} records.")
        return result_df

    except Exception as e:
        logger.error(f"[Transform] Critical logic failure: {str(e)}")
        raise