import os

import pandas as pd
import pytest

from utils.config import config


@pytest.fixture(scope="module")
def df():
    """
    Load the processed dataset fixture.
    Scope='module' ensures the file is read only once per test session.

    If the file does not exist, tests will be skipped automatically.
    """
    file_path = config["path"]["processed_data"]

    if not os.path.exists(file_path):
        pytest.skip(f"Skipping tests: Data file not found at {file_path}")

    return pd.read_csv(file_path)


def test_data_schema(df):
    """
    Verify that the dataset contains necessary columns for analysis.
    Note: 'summonerName' is no longer returned by Riot API v4.
    We use 'puuid' as the unique identifier.
    """
    required_columns = ["puuid", "leaguePoints", "wins", "losses"]

    for col in required_columns:
        assert col in df.columns, f"Missing required column: {col}"


def test_no_data_leakage(df):
    """
    Critical: Ensure 'gold_earned' and 'total_damage' are REMOVED.
    These columns cause data leakage in win prediction models.
    """
    forbidden_cols = ["gold_earned", "total_damage"]
    for col in forbidden_cols:
        assert col not in df.columns, f"Data Leakage detected: {col} should be dropped."


def test_no_missing_values(df):
    """
    Ensure KNN Imputation worked correctly.
    Numeric columns should not have any NaN values.
    """
    numeric_df = df.select_dtypes(include=["number"])
    assert (
        numeric_df.isnull().sum().sum() == 0
    ), "Found missing values in numeric columns after imputation."


def test_business_logic_integrity(df):
    """
    Validate domain-specific constraints.
    Constraint: Total games cannot be negative.
    """
    # Calculate derived metric if not exists
    if "total_games" not in df.columns:
        total_games = df["wins"] + df["losses"]
    else:
        total_games = df["total_games"]

    assert (total_games >= 0).all(), "Found users with negative game counts."
