import requests
import json
import os
from dotenv import load_dotenv

# .env 파일에서 API 키 가져오기
load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
print(f" 현재 읽힌 키: {API_KEY}")

def get_challenger_league():
    # 1. API 호출 (메타코드에서 배운 requests 사용)
    url = f"https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # 2. 데이터 저장 (data/raw 폴더에)
        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/challenger_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("✅ 데이터 수집 완료!")
    else:
        print(f"❌ 에러 발생: {response.status_code}")
        if response.status_code == 429:
            print("⏳ API 요청 제한(Rate Limit)에 걸렸습니다. 잠시 후 다시 시도하세요.")
        elif response.status_code == 403:
            print("🔑 API 키가 만료되었거나 잘못되었습니다. .env 파일을 확인하세요.")

if __name__ == "__main__":
    get_challenger_league()