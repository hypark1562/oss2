"""
Module: tests/test_data_quality.py
Description: Unit tests for data validation logic.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl.transform import validate_data
from utils.config import load_config


@pytest.fixture
def config():
    """Load configuration for tests."""
    return load_config()


@pytest.fixture
def valid_dataframe():
    """Create a sample valid DataFrame."""
    data = {
        "player_name": ["Faker", "Chovy"],
        "summoner_id": ["id1", "id2"],
        "lp": [1200, 1100],
        "wins": [100, 90],
        "losses": [50, 45],
        "win_rate": [66.6, 66.6],
    }
    return pd.DataFrame(data)


def test_validate_data_success(valid_dataframe):
    """Test that valid data passes validation."""
    assert validate_data(valid_dataframe) is True


def test_validate_data_failure_logic(valid_dataframe):
    """Test that impossible data (Win Rate > 100%) fails validation."""

    valid_dataframe.loc[0, "win_rate"] = 150.0
    assert validate_data(valid_dataframe) is False


def test_validate_data_failure_missing_columns(valid_dataframe):
    """Test that missing critical columns fails validation."""

    invalid_df = valid_dataframe.drop(columns=["player_name"])
    assert validate_data(invalid_df) is False


def test_empty_dataframe():
    """Test that empty DataFrame fails validation."""
    empty_df = pd.DataFrame()
    assert validate_data(empty_df) is False
