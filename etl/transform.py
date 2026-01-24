"""
Module: transform.py
Description: Implementation of data cleansing, validation, and feature engineering.
             Ensures data integrity before the loading phase by handling schema
             drifts and logical anomalies.
"""

import logging

import numpy as np
import pandas as pd

# Initialize logger for centralized monitoring
logger = logging.getLogger(__name__)


def validate_data(df: pd.DataFrame) -> bool:
    """
    Performs Data Quality(DQ) checks to prevent downstream model contamination.
    Returns True if data passes all integrity constraints.
    """
    if df.empty:
        logger.warning("[Validation] Empty DataFrame detected.")
        return False

    # 1. Business Logic Check: Validate metric boundaries
    invalid_logic = df[(df["win_rate"] > 100) | (df["lp"] < 0)]

    if not invalid_logic.empty:
        logger.error(
            f"[Validation] Logic violation detected in {len(invalid_logic)} rows."
        )
        return False

    # 2. Schema Integrity Check: Ensure critical identifiers exist
    critical_fields = ["player_name", "summoner_id", "lp"]
    if df[critical_fields].isnull().any().any():
        logger.error("[Validation] Null values found in critical fields.")
        return False

    logger.info(f"[Validation] Data quality check passed for {len(df)} records.")
    return True


def transform_data(raw_data: list) -> pd.DataFrame:
    """
    Standardize raw API response into a structured DataFrame with feature engineering.
    Includes defensive logic for missing columns (KeyError prevention).
    """
    try:
        # Initial payload verification
        if not raw_data:
            logger.error("[Transform] Null or empty payload received.")
            raise ValueError("TRANSFORM_INPUT_EMPTY")

        df = pd.DataFrame(raw_data)
        logger.info(f"[Transform] Initializing transformation for {len(df)} records.")

        # 1. Dynamic Schema Mapping (Riot API -> Internal System)
        schema_map = {
            "summonerName": "player_name",
            "summonerId": "summoner_id",
            "leaguePoints": "lp",
            "wins": "wins",
            "losses": "losses",
        }

        # [Defensive Logic] Handle schema drift by checking column existence
        for api_key, internal_name in schema_map.items():
            if api_key not in df.columns:
                logger.warning(
                    f"[Transform] Missing key '{api_key}'. Filling with default values."
                )
                df[internal_name] = "Unknown" if "name" in internal_name else 0
            else:
                df.rename(columns={api_key: internal_name}, inplace=True)

        # 2. Column Selection
        target_columns = list(schema_map.values())
        df = df[target_columns]

        # 3. Feature Engineering
        df["total_games"] = df["wins"] + df["losses"]
        df["win_rate"] = np.where(
            df["total_games"] > 0, (df["wins"] / df["total_games"] * 100).round(2), 0.0
        )

        # 4. Final DQ Gate
        if not validate_data(df):
            logger.error("[Transform] Data Quality validation failed.")
            raise ValueError("DATA_QUALITY_VALIDATION_FAILED")

        return df.sort_values(by="lp", ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(
            f"[Transform] Critical logic failure: {type(e).__name__} - {str(e)}"
        )
        raise
