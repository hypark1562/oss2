import json
import logging
import os

import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer

from utils.config import config

logger = logging.getLogger(__name__)


def process_data() -> bool:
    """
    Transform Raw Data (JSON) into Processed Data (CSV).

    Key Engineering Decisions:
        1. PyArrow Backend: Optimized for memory efficiency and speed.
        2. Data Leakage Prevention: Removed columns (e.g., gold_earned) that directly imply the game result.
        3. KNN Imputation: Filled missing values based on user similarity (k-NN) rather than simple mean.

    Returns:
        bool: True if transformation is successful, False otherwise.
    """
    logger.info("[Transform] Start processing pipeline...")

    input_path = config["path"]["raw_data"]
    output_path = config["path"]["processed_data"]
    output_dir = os.path.dirname(output_path)

    # Validation: Check if raw data exists
    if not os.path.exists(input_path):
        logger.error(f"[Transform] Input file not found: {input_path}")
        return False

    try:
        # 1. Load Data
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if "entries" not in raw_data:
            logger.error("[Transform] Invalid JSON structure: 'entries' key missing")
            return False

        df = pd.DataFrame(raw_data["entries"])

        # 2. Optimize Memory with PyArrow (Pandas 2.0+)
        try:
            df = df.convert_dtypes(dtype_backend="pyarrow")
        except Exception as e:
            logger.warning(
                f"[Transform] PyArrow conversion failed, fallback to NumPy: {e}"
            )

        # 3. Remove Data Leakage Columns
        leakage_cols = ["gold_earned", "total_damage"]
        cols_to_drop = [c for c in leakage_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # 4. Handle Missing Values (KNN Imputation)
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(df) > 5 and len(numeric_cols) > 0:
            k = config["settings"]["knn_neighbors"]
            imputer = KNNImputer(n_neighbors=k)

            # Convert to numpy for performance
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols].to_numpy())
            logger.info(f"[Integrity] Imputed missing values using KNN (k={k})")

        # 5. Save Processed Data
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_path, index=False)

        logger.info(f"[Transform] Data cleaning completed: {output_path}")
        return True

    except Exception as e:
        logger.exception(f"[Transform] Critical Processing Error: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_data()
