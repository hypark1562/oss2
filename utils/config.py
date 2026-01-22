import os

import yaml


def load_config():
    config_path = "config.yaml"

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ 설정 파일이 없습니다: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


config = load_config()
