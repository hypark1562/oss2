"""
Slack Notification Utility Module.
Provides real-time monitoring alerts using Slack Webhooks.
Designed to deliver structured incident reports to the engineering team.
"""

import json
import os
import logging
from datetime import datetime
import requests
from typing import Optional

# 로거 설정
logger = logging.getLogger(__name__)

def send_slack_alert(message: str, level: str = "INFO") -> None:
    """
    파이프라인의 실행 상태나 장애 내역을 Slack 채널로 전송합니다.
    
    Args:
        message (str): 알림 본문 내용
        level (str): 알림의 심각도 수준 (INFO, WARNING, ERROR, CRITICAL)
    """
    
    # 1. Configuration: 환경 변수에서 Webhook URL 보안 로드
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        logger.warning("[Alert] SLACK_WEBHOOK_URL is missing. Skipping notification.")
        return

    # 2. Visual Styling: 등급별 시각적 요소(색상, 이모지) 정의
    # 실무에서는 색상만으로도 상황의 위급함을 즉시 인지할 수 있어야 합니다.
    severity_map = {
        "INFO": {"color": "#36a64f", "emoji": "✅", "title": "System Normal"},
        "WARNING": {"color": "#FFCC00", "emoji": "⚠️", "title": "System Warning"},
        "ERROR": {"color": "#FF0000", "emoji": "🚨", "title": "System Error"},
        "CRITICAL": {"color": "#800000", "emoji": "🔥", "title": "Critical Failure"}
    }
    
    config = severity_map.get(level.upper(), severity_map["INFO"])

    # 3. Payload Construction: Slack 'Attachments' 레이아웃 구성
    # 단순 텍스트보다 필드 형식을 사용하면 로그 데이터 등을 깔끔하게 보여줄 수 있습니다.
    payload = {
        "attachments": [
            {
                "fallback": f"[{level}] {message}",
                "color": config["color"],
                "pretext": f"{config['emoji']} *LoL Pipeline Monitoring*",
                "title": config["title"],
                "text": message,
                "fields": [
                    {
                        "title": "Environment",
                        "value": "Production",
                        "short": True
                    },
                    {
                        "title": "Timestamp",
                        "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "short": True
                    }
                ],
                "footer": "ETL-Bot-v1.0",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }

    # 4. Transmission: HTTP POST 요청을 통한 메시지 발송
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=5 # 알림 전송 지연이 전체 파이프라인에 영향을 주지 않도록 짧은 타임아웃 설정
        )

        if response.status_code != 200:
            logger.error(f"[Alert] Slack API returned error: {response.status_code} - {response.text}")
        else:
            logger.debug(f"[Alert] Notification sent successfully (Level: {level})")

    except requests.exceptions.RequestException as e:
        # 알림 전송 실패가 메인 로직을 중단시켜서는 안 되므로 에러 로깅 후 통과
        logger.error(f"[Alert] Failed to connect to Slack Webhook: {str(e)}")