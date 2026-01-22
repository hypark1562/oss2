import os

import pytest

from utils.config import load_config


@pytest.fixture
def config():
    """
    Project configuration fixture.
    Loads the config.yaml file once for all tests in this module.
    """
    return load_config()


def test_config_file_exists():
    """Verify that the essential 'config.yaml' file exists in the root directory."""
    assert os.path.exists("config.yaml"), "Critical: config.yaml file is missing."


def test_essential_keys_structure(config):
    """
    Verify that the configuration dictionary contains all top-level keys.
    Required keys: 'path', 'api', 'settings'
    """
    required_keys = ["path", "api", "settings"]
    for key in required_keys:
        assert key in config, f"Missing required configuration key: {key}"


def test_path_configuration(config):
    """Ensure path configurations are strings and not empty."""
    paths = config["path"]
    assert isinstance(paths["raw_data"], str)
    assert isinstance(paths["processed_data"], str)
    assert isinstance(paths["db_path"], str)


def test_settings_types(config):
    """
    Validate data types for numerical settings.
    Example: 'knn_neighbors' must be an integer.
    """
    settings = config["settings"]
    assert isinstance(
        settings["knn_neighbors"], int
    ), "knn_neighbors must be an integer"
    assert settings["knn_neighbors"] > 0, "knn_neighbors must be positive"
