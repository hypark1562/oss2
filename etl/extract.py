import json
import logging
import os

import requests
from dotenv import load_dotenv

from utils.config import config

logger = logging.getLogger(__name__)

# 환경변수 로드
load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")


def get_challenger_league() -> bool:
    """
    Riot API로부터 챌린저 티어 유저 데이터를 수집하여 Raw Layer에 적재합니다.

    Returns:
        bool: 수집 및 저장 성공 여부
    """
    # API URL 구성
    base_url = config["api"]["challenger_url"]
    request_url = f"{base_url}?api_key={API_KEY}"
    save_path = config["path"]["raw_data"]

    try:
        logger.info("🔄 [Extract] Requesting data from Riot API...")
        response = requests.get(request_url)

        # 1. 정상 응답 처리 (200 OK)
        if response.status_code == 200:
            data = response.json()

            # 디렉토리 확인 및 생성
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            logger.info(f"✅ [Extract] Saved raw data to: {save_path}")
            return True

        # 2. 에러 핸들링
        else:
            logger.error(
                f"❌ [Extract] API Request Failed: Status {response.status_code}"
            )

            if response.status_code == 429:
                logger.warning("⏳ Rate Limit Exceeded. Please retry later.")
            elif response.status_code == 403:
                logger.critical("🔑 Unauthorized. Check your RIOT_API_KEY in .env")

            return False

    except Exception as e:
        logger.exception(f"❌ [Extract] Unexpected Error: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    get_challenger_league()
