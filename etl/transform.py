import logging

import pandas as pd

# 로거 설정
logger = logging.getLogger(__name__)


def transform_data(raw_data):
    """
    수집된 Raw Data(리스트)를 정제하여 분석 가능한 DataFrame으로 변환합니다.

    Args:
        raw_data (list): extract 단계에서 넘겨받은 딕셔너리 리스트

    Returns:
        pd.DataFrame: 전처리가 완료된 데이터프레임
    """
    try:
        logger.info("[Transform] Starting data transformation...")

        # 1. 데이터가 비어있는지 확인
        if not raw_data:
            raise ValueError("No data available to transform")

        # 2. Pandas DataFrame으로 변환
        df = pd.DataFrame(raw_data)

        # 3. 불필요한 컬럼 제거 (필요한 것만 남기기)
        # (API 버전에 따라 컬럼명이 다를 수 있으니, 주요 컬럼만 선택)
        target_columns = [
            "summonerId",
            "summonerName",
            "leaguePoints",
            "wins",
            "losses",
            "veteran",
            "inactive",
            "freshBlood",
            "hotStreak",
        ]

        # 실제 데이터에 있는 컬럼만 골라내기 (에러 방지)
        available_cols = [col for col in target_columns if col in df.columns]
        df = df[available_cols]

        # 4. 파생 변수 생성 (Feature Engineering) - 승률 계산
        # 승률 = 승리 / (승리 + 패배) * 100
        df["totalGames"] = df["wins"] + df["losses"]
        df["winRate"] = round((df["wins"] / df["totalGames"]) * 100, 2)

        # 5. 결측치 처리 (혹시 모를 빈 값 채우기)
        df = df.fillna(0)

        # 6. 점수 높은 순으로 정렬
        df = df.sort_values(by="leaguePoints", ascending=False).reset_index(drop=True)

        logger.info(f"[Transform] Successfully processed {len(df)} rows.")
        return df

    except Exception as e:
        logger.error(f"[Transform] Error during transformation: {e}")
        raise e
