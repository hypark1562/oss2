"""
Module: utils.config
Description: Utility to load YAML configuration files.
"""
import os

import yaml


def load_config(config_path="config.yaml"):
    """
    Loads configuration from a YAML file.
    """
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_path, config_path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Configuration file not found at: {full_path}")

    with open(full_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
