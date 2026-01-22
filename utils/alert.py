import json
import os
from datetime import datetime

import requests


def send_slack_alert(message: str, level: str = "INFO"):
    """
    Slack으로 알림을 보내는 함수입니다.

    Args:
        message (str): 보낼 메시지 내용
        level (str): 알림 등급 ("INFO"는 초록색, "ERROR"는 빨간색으로 표시)
    """
    # 1. .env 파일에서 주소를 몰래 가져옵니다.
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    # 주소가 없으면(설정 안 했으면) 그냥 넘어갑니다.
    if not webhook_url:
        print("⚠️ 경고: SLACK_WEBHOOK_URL이 설정되지 않았습니다. 알림을 건너뜁니다.")
        return

    # 2. 메시지 색상 정하기 (에러면 빨간색, 성공이면 초록색)
    if level == "ERROR":
        color = "#FF0000"  # 빨간색 (위험!)
        emoji = "🚨"
    else:
        color = "#36a64f"  # 초록색 (안전)
        emoji = "✅"

    # 3. 슬랙이 알아듣는 포맷(JSON)으로 편지 쓰기
    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"{emoji} [{level}] 데이터 파이프라인 알림",
                "text": message,
                "footer": "LoL Data Pipeline System",
                "ts": datetime.now().timestamp(),
            }
        ]
    }

    # 4. 우체부(requests)를 통해 편지 보내기
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        # 잘 안 갔으면 에러 출력
        if response.status_code != 200:
            print(f"❌ 슬랙 전송 실패: {response.text}")

    except Exception as e:
        print(f"❌ 슬랙 전송 중 에러 발생: {e}")
