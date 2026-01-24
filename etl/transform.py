"""
Module: etl.transform
Description:
    Performs data cleansing, schema standardization, and feature engineering.
    Ensures data quality (DQ) before loading into the production database.
"""
import logging
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def validate_data(df: pd.DataFrame) -> bool:
    """
    Executes Data Quality (DQ) checks on the transformed DataFrame.

    Checks:
        1. Logical bounds (e.g., Win Rate <= 100%).
        2. Schema integrity (Critical columns existence).
        3. Null constraints on primary keys.

    Args:
        df (pd.DataFrame): The DataFrame to validate.

    Returns:
        bool: True if all checks pass, False otherwise.
    """
    if df.empty:
        logger.warning("[Validation] DataFrame is empty. Skipping validation.")
        return False

    # 1. Business Logic Validation
    if "win_rate" in df.columns and "lp" in df.columns:
        invalid_logic = df[(df["win_rate"] > 100) | (df["lp"] < 0)]
        if not invalid_logic.empty:
            logger.error(
                f"[Validation] Logic violation detected in {len(invalid_logic)} rows."
            )
            return False

    # 2. Schema Integrity & Null Checks
    critical_fields = ["player_name", "summoner_id", "lp"]

    # [Defensive] Check if critical columns exist before accessing them
    missing_cols = [col for col in critical_fields if col not in df.columns]
    if missing_cols:
        logger.error(
            f"[Validation] Critical schema drift detected. Missing: {missing_cols}"
        )
        return False

    if df[critical_fields].isnull().any().any():
        logger.error("[Validation] Null values found in critical identifier fields.")
        return False

    logger.info(f"[Validation] DQ checks passed for {len(df)} records.")
    return True


def transform_data(raw_data: List[dict]) -> pd.DataFrame:
    """
    Transforms raw JSON data into a structured format for analytics.

    Features:
        - Dynamic Schema Mapping (CamelCase -> snake_case).
        - Handling of missing columns via default value injection.
        - Calculation of derived metrics (Win Rate).

    Args:
        raw_data (List[dict]): List of raw player dictionaries from API.

    Returns:
        pd.DataFrame: A cleaned and sorted DataFrame ready for loading.

    Raises:
        ValueError: If input is empty or DQ checks fail.
    """
    try:
        if not raw_data:
            logger.error("[Transform] Input payload is empty.")
            raise ValueError("TRANSFORM_INPUT_EMPTY")

        df = pd.DataFrame(raw_data)
        logger.info(f"[Transform] Processing {len(df)} raw records.")

        # [Schema Mapping] Define explicit mapping for internal standardization
        schema_map = {
            "summonerName": "player_name",
            "summonerId": "summoner_id",
            "leaguePoints": "lp",
            "wins": "wins",
            "losses": "losses",
        }

        # [Defensive] Handle Schema Drift (API changes)
        for api_key, internal_name in schema_map.items():
            if api_key not in df.columns:
                logger.warning(
                    f"[Transform] Schema mismatch: '{api_key}' missing. Filling default."
                )
                df[internal_name] = "Unknown" if "name" in internal_name else 0
            else:
                df.rename(columns={api_key: internal_name}, inplace=True)

        # Feature Selection & Engineering
        df = df[list(schema_map.values())]
        df["total_games"] = df["wins"] + df["losses"]

        # Calculate Win Rate (Handle division by zero)
        df["win_rate"] = np.where(
            df["total_games"] > 0, (df["wins"] / df["total_games"] * 100).round(2), 0.0
        )

        # Final DQ Gate
        if not validate_data(df):
            logger.error("[Transform] Data Validation failed. Pipeline aborted.")
            raise ValueError("DATA_QUALITY_FAILURE")

        return df.sort_values(by="lp", ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(f"[Transform] Internal error: {str(e)}", exc_info=True)
        raise
