import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


"""
Module: transform.py
Cleanses raw API response and performs feature engineering (Win Rates, etc.).
"""

def transform_data(raw_data):
    """
    Transforms raw list to validated DataFrame.
    Includes data cleaning and KDA/WinRate feature generation.
    """
    # Validation: Ensure input contains processable data
    if not raw_data:
        raise ValueError("Input data is empty. Cannot proceed to transform.")

    df = pd.DataFrame(raw_data)

    # Schema mapping for database consistency
    schema = {
        "summonerName": "player_name",
        "leaguePoints": "lp",
        "wins": "wins",
        "losses": "losses"
    }
    df = df[list(schema.keys())].rename(columns=schema)

    # Calculate derivative features
    # Note: Use numpy to handle potential ZeroDivision scenarios
    df["total_games"] = df["wins"] + df["losses"]
    df["win_rate"] = np.where(
        df["total_games"] > 0,
        (df["wins"] / df["total_games"] * 100).round(2),
        0.0
    )

    # Sorting by performance metric (LP)
    return df.sort_values(by="lp", ascending=False).reset_index(drop=True)