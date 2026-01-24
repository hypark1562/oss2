"""
Module: utils.config
Description:
    Provides a centralized interface for loading configuration files.
    Ensures path resolution is robust across different execution environments.
"""
import os
from typing import Any, Dict

import yaml


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Parses the YAML configuration file.

    Args:
        config_path (str): Relative path to the config file.

    Returns:
        dict: Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    # Resolve absolute path relative to the project root
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_path, config_path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"[Config] File not found at: {full_path}")

    with open(full_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
