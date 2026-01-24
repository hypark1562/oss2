"""
Module: tests/test_config.py
Description: Unit tests for configuration file structure.
"""
import os
import sys

import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

try:
    from utils.config import load_config
except ImportError:
    load_config = None


@pytest.fixture
def config():
    """
    Project configuration fixture.
    Loads the config.yaml file once for all tests in this module.
    """
    if not load_config:
        pytest.fail("Could not import load_config from utils.config")
    return load_config()


def test_config_file_exists():
    """Verify that the essential 'config.yaml' file exists in the root directory."""
    assert os.path.exists("config.yaml"), "Critical: config.yaml file is missing."


def test_essential_keys_structure(config):
    """
    Verify that the configuration dictionary contains top-level keys.
    Note: 'settings' key is removed as KNN logic was deprecated.
    Required keys: 'path', 'api'
    """
    required_keys = ["path", "api"]
    for key in required_keys:
        assert key in config, f"Missing required configuration key: {key}"


def test_path_configuration(config):
    """Ensure path configurations are strings and not empty."""
    paths = config["path"]

    assert isinstance(paths.get("raw_data"), str)
    assert isinstance(paths.get("processed_data"), str)
    assert isinstance(paths.get("db_path"), str)
