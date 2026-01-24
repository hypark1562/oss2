"""
Module: transform.py
Description: Implementation of data cleansing, validation, and feature engineering.
             Ensures data integrity before the loading phase.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def validate_data(df: pd.DataFrame) -> bool:
    """
    Performs Data Quality(DQ) checks to prevent downstream model contamination.
    Returns True if data passes all integrity constraints.
    """
    if df.empty:
        logger.warning("[Validation] Empty DataFrame detected.")
        return False

    # Check for logical anomalies in core metrics
    # 1. Win rate must be within 0-100 range
    # 2. LP(League Points) should not be negative
    invalid_logic = df[(df["win_rate"] > 100) | (df["lp"] < 0)]

    if not invalid_logic.empty:
        logger.error(
            f"[Validation] Logic violation detected in {len(invalid_logic)} rows."
        )
        return False

    # Check for critical Null values in primary features
    critical_fields = ["player_name", "summoner_id", "lp"]
    if df[critical_fields].isnull().any().any():
        logger.error("[Validation] Null values found in critical fields.")
        return False

    logger.info("[Validation] Data quality check passed.")
    return True


def transform_data(raw_data):
    """
    Standardize raw API response into a structured DataFrame with feature engineering.
    """
    try:
        if not raw_data:
            logger.error("[Transform] Null payload received.")
            raise ValueError("TRANSFORM_INPUT_EMPTY")

        df = pd.DataFrame(raw_data)

        # Dynamic Schema Mapping
        schema = {
            "summonerName": "player_name",
            "summonerId": "summoner_id",
            "leaguePoints": "lp",
            "wins": "wins",
            "losses": "losses",
        }

        actual_columns = [col for col in schema.keys() if col in df.columns]
        df = df[actual_columns].rename(
            columns={k: v for k, v in schema.items() if k in actual_columns}
        )

        # Feature Engineering: Defensive numeric handling
        for col in ["wins", "losses"]:
            if col not in df.columns:
                df[col] = 0

        df["total_games"] = df["wins"] + df["losses"]
        df["win_rate"] = np.where(
            df["total_games"] > 0, (df["wins"] / df["total_games"] * 100).round(2), 0.0
        )

        # Final DQ Check before returning to orchestrator
        if not validate_data(df):
            raise ValueError("DATA_QUALITY_VALIDATION_FAILED")

        return df.sort_values(by="lp", ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(f"[Transform] Critical logic failure: {str(e)}")
        raise
