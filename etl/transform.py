"""
Module: transform.py
Description: Data Transformation & Quality Assurance Layer.
             This module is responsible for standardizing raw JSON data from the Riot API
             into a structured DataFrame. It implements a 'Defensive Programming' strategy
             to handle unexpected schema drifts and ensures data integrity through
             rigorous DQ (Data Quality) gates.
"""

import logging

import numpy as np
import pandas as pd

# Initialize logger for centralized monitoring and observability
logger = logging.getLogger(__name__)


def validate_data(df: pd.DataFrame) -> bool:
    """
    Executes Data Quality (DQ) checks to serve as a firewall before DB loading.

    Checks performed:
    1. Volumetric Check: Ensure data is not empty.
    2. Business Logic Validation: logical boundaries (e.g., Win Rate <= 100).
    3. Schema Contract: Verify critical columns exist.
    4. Completeness: Check for null values in mandatory fields.

    Args:
        df (pd.DataFrame): Transformed DataFrame ready for validation.

    Returns:
        bool: True if data passes all checks, False otherwise.
    """
    # [DQ Check 1] Volumetric Validation (Fail-Fast)
    if df.empty:
        logger.warning("[Validation] Dataset is empty. Skipping downstream processing.")
        return False

    # [DQ Check 2] Business Logic & Outlier Detection
    # Ensure logical consistency of metrics (e.g., LP cannot be negative, WR <= 100%)
    if "win_rate" in df.columns and "lp" in df.columns:
        invalid_logic = df[(df["win_rate"] > 100) | (df["lp"] < 0)]
        if not invalid_logic.empty:
            logger.error(
                f"[Validation] Business logic violation detected in {len(invalid_logic)} rows. "
                "Possible cause: Raw data corruption or calculation error."
            )
            return False

    # [DQ Check 3] Schema Contract Validation
    # Critical fields must exist for the pipeline to function correctly.
    critical_fields = ["player_name", "summoner_id", "lp"]

    # Defensive Check: Verify column existence before accessing them to prevent KeyError
    missing_cols = [col for col in critical_fields if col not in df.columns]
    if missing_cols:
        logger.error(
            f"[Validation] Schema violation: Missing critical columns {missing_cols}"
        )
        return False

    # [DQ Check 4] Completeness Validation
    # Null values in Primary Key candidates or critical metrics are not allowed.
    if df[critical_fields].isnull().any().any():
        logger.error(
            "[Validation] Data Integrity Error: Null values found in critical fields."
        )
        return False

    logger.info(f"[Validation] Data Quality Gate passed. Valid records: {len(df)}")
    return True


def transform_data(raw_data: list) -> pd.DataFrame:
    """
    Orchestrates the transformation pipeline: Raw JSON -> Cleaned DataFrame.

    Key Engineering Decisions:
    - **Defensive Mapping:** Decouples API response keys from internal schema to survive upstream API changes.
    - **Vectorization:** Uses NumPy/Pandas native operations for high-performance feature engineering.
    - **Availability Priority:** Fills missing non-critical fields with defaults instead of crashing the pipeline.

    Args:
        raw_data (list): List of dictionaries from Riot API response.

    Returns:
        pd.DataFrame: A structured, validated, and sorted DataFrame.

    Raises:
        ValueError: If input is empty or DQ validation fails.
    """
    try:
        # [Phase 0] Input Guard clauses
        if not raw_data:
            logger.error("[Transform] Input payload is null or empty.")
            raise ValueError("TRANSFORM_INPUT_EMPTY")

        df = pd.DataFrame(raw_data)
        logger.info(f"[Transform] Started processing {len(df)} raw records.")

        # [Phase 1] Dynamic Schema Mapping (Decoupling Layer)
        # Maps external API keys to internal standardized column names.
        schema_map = {
            "summonerName": "player_name",
            "summonerId": "summoner_id",
            "leaguePoints": "lp",
            "wins": "wins",
            "losses": "losses",
        }

        # [Defensive Logic] Handling Schema Drift
        # If the API removes a field, we inject default values to maintain pipeline availability.
        for api_key, internal_name in schema_map.items():
            if api_key not in df.columns:
                logger.warning(
                    f"[Transform] Schema Drift Detected: '{api_key}' is missing. "
                    f"Injecting default value for '{internal_name}' to ensure continuity."
                )
                # Assign 'Unknown' for string fields (names), 0 for numeric fields
                df[internal_name] = "Unknown" if "name" in internal_name else 0
            else:
                df.rename(columns={api_key: internal_name}, inplace=True)

        # [Phase 2] Column Selection & Isolation
        # Retain only necessary features to optimize memory usage (Projection).
        target_columns = list(schema_map.values())
        df = df[target_columns]

        # [Phase 3] Feature Engineering (Vectorized)
        # Avoid row-wise loops. Use vectorized operations for CPU efficiency.
        df["total_games"] = df["wins"] + df["losses"]

        # Calculate Win Rate with zero-division handling
        df["win_rate"] = np.where(
            df["total_games"] > 0, (df["wins"] / df["total_games"] * 100).round(2), 0.0
        )

        # [Phase 4] Final Data Quality Gate
        # Ensure the transformed data is safe to load into the Database.
        if not validate_data(df):
            logger.error("[Transform] Critical DQ failure. Aborting transaction.")
            raise ValueError("DATA_QUALITY_VALIDATION_FAILED")

        # [Phase 5] Sorting & Final Polish
        # Sort by LP (descending) to align with business context (Rankings).
        return df.sort_values(by="lp", ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(
            f"[Transform] Unhandled exception during transformation: {type(e).__name__} - {str(e)}",
            exc_info=True,
        )
        raise
