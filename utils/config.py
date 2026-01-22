import os
from typing import Any, Dict

import yaml


def load_config() -> Dict[str, Any]:
    """
    Load configuration settings from a YAML file.

    This function reads 'config.yaml' from the project root.
    It serves as a single source of truth for paths, API URLs, and parameters.

    Returns:
        Dict[str, Any]: Configuration dictionary.

    Raises:
        FileNotFoundError: If 'config.yaml' does not exist.
    """
    config_path = "config.yaml"

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# Global configuration object to be imported by other modules
config = load_config()
