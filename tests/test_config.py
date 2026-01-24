"""
Module: tests/test_config.py
Description: Unit tests for configuration file structure.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from utils.config import load_config
except ImportError:
    load_config = None


@pytest.fixture
def config():
    """
    Project configuration fixture.
    """
    if not load_config:
        pytest.fail(
            "Could not import load_config from utils.config. Check if PyYAML is installed and utils/config.py exists."
        )
    return load_config()


def test_config_file_exists():
    """Verify that 'config.yaml' exists."""
    assert os.path.exists("config.yaml"), "config.yaml file is missing in root."


def test_essential_keys_structure(config):
    """
    Verify configuration keys.
    """
    required_keys = ["path", "api"]
    for key in required_keys:
        assert key in config, f"Missing required configuration key: {key}"


def test_path_configuration(config):
    """Ensure path configurations are strings."""
    paths = config["path"]
    assert isinstance(paths.get("raw_data"), str)
    assert isinstance(paths.get("processed_data"), str)
    assert isinstance(paths.get("db_path"), str)
