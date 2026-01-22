import json
import logging
import os

import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer

from utils.config import config

logger = logging.getLogger(__name__)


def process_data() -> bool:
    """
    Raw Data(JSON)를 로드하여 분석 가능한 형태(CSV)로 정제합니다.

    Key Features:
        - PyArrow 백엔드를 통한 메모리 최적화
        - Data Leakage 유발 컬럼 제거 (Business Logic)
        - KNN 기반의 결측치 보간 (Data Integrity)

    Returns:
        bool: 프로세스 성공 여부
    """
    logger.info("🔄 [Transform] 데이터 전처리 시작")

    # Config 로드 (Local Scope)
    input_path = config["path"]["raw_data"]
    output_path = config["path"]["processed_data"]
    output_dir = os.path.dirname(output_path)

    # 1. 유효성 검사
    if not os.path.exists(input_path):
        logger.error(f"❌ Input file not found: {input_path}")
        return False

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if "entries" not in raw_data:
            logger.error("❌ Invalid JSON structure: 'entries' key missing")
            return False

        # 2. DataFrame 변환 및 최적화
        df = pd.DataFrame(raw_data["entries"])

        # Pandas 2.0 PyArrow 백엔드 적용 (메모리 효율화)
        try:
            df = df.convert_dtypes(dtype_backend="pyarrow")
        except Exception as e:
            logger.warning(f"⚠️ PyArrow conversion failed, falling back to NumPy: {e}")

        # 3. 비즈니스 로직 적용 (Data Leakage 방지)
        # 승패와 직접적인 연관이 있는 사후 지표(골드 획득량 등) 제거
        leakage_cols = ["gold_earned", "total_damage"]
        cols_to_drop = [c for c in leakage_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # 4. 결측치 처리 (KNN Imputation)
        # 단순 평균 대치 대신, 유사 유저 그룹(Neighbor)의 패턴을 기반으로 보간
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(df) > 5 and len(numeric_cols) > 0:
            k = config["settings"]["knn_neighbors"]
            imputer = KNNImputer(n_neighbors=k)

            # KNN 연산을 위해 numpy 배열로 변환
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols].to_numpy())
            logger.info(f"✅ Imputed missing values using KNN (k={k})")

        # 5. 결과 저장
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_path, index=False)

        logger.info(f"✅ [Transform] Completed: {output_path}")
        return True

    except Exception as e:
        logger.exception(f"❌ [Transform] Critical Error: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_data()
