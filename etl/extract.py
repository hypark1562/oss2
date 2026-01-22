import json
import logging
import os

import requests
from dotenv import load_dotenv

# 로거 설정
logger = logging.getLogger(__name__)

# 환경변수 로드
load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")


def extract_data():
    """
    Riot API에서 챌린저 데이터를 가져와서 반환합니다.
    (기존 get_challenger_league 함수를 파이프라인용으로 수정)
    """
    # 1. API 키 확인
    if not API_KEY:
        logger.error("API Key is missing!")
        raise ValueError("RIOT_API_KEY not found in .env file")

    # 2. 요청 URL 만들기 (KR 서버 챌린저 랭크)
    base_url = "https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
    headers = {"X-Riot-Token": API_KEY}

    try:
        logger.info("[Extract] Requesting data from Riot API...")
        response = requests.get(base_url, headers=headers)

        # 3. 성공 시 데이터 반환
        if response.status_code == 200:
            data = response.json()

            # (선택) 원본 데이터 파일로 백업 저장
            os.makedirs("data/raw", exist_ok=True)
            with open("data/raw/challenger_raw.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            logger.info(f"[Extract] Success! Collected {len(data['entries'])} entries.")

            # ★핵심★: True/False가 아니라 '데이터(리스트)'를 반환해야 Transform 단계로 넘어갑니다.
            return data["entries"]

        # 4. 에러 처리
        elif response.status_code == 403:
            logger.critical("[Extract] 403 Unauthorized. API Key expired?")
            raise Exception("API Key Expired")
        elif response.status_code == 429:
            logger.warning("[Extract] 429 Rate Limit Exceeded.")
            raise Exception("Rate Limit Exceeded")
        else:
            logger.error(f"[Extract] Request Failed: {response.status_code}")
            raise Exception(f"API Request Failed: {response.status_code}")

    except Exception as e:
        logger.exception(f"[Extract] System Error: {e}")
        raise e
