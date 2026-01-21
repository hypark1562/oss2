import requests
import json
import os
import logging
import time
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")

def get_challenger_league() -> bool:
    """
    Riot API에서 챌린저 리그 정보를 수집하여 JSON으로 저장합니다.
    Returns:
        bool: 수집 성공 여부
    """
    url = f"https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key={API_KEY}"
    
    try:
        logger.info("🔄 [Extract] Riot API 데이터 요청 시작...")
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            
            os.makedirs("data/raw", exist_ok=True)
            save_path = "data/raw/challenger_data.json"
            
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            logger.info(f"✅ [Extract] 데이터 수집 및 저장 완료: {save_path}")
            return True
            
        else:
            logger.error(f"❌ [Extract] API 에러 발생: {response.status_code}")
            
            # [Risk Defense] Rate Limit 대응 로직
            if response.status_code == 429:
                logger.warning("⏳ API 요청 제한(Rate Limit) 감지. 잠시 대기가 필요합니다.")
            elif response.status_code == 403:
                logger.critical("🔑 API 키 만료 또는 권한 없음. .env 확인 필요.")
            
            return False
            
    except Exception as e:
        logger.exception(f"❌ [Extract] 알 수 없는 에러 발생: {e}")
        return False

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO)
    get_challenger_league()