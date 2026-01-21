import pandas as pd
import numpy as np
import os
import json
import logging
from sklearn.impute import KNNImputer

logger = logging.getLogger(__name__)

def process_data() -> bool:
    """
    Raw Data(JSON)를 로드하여 전처리(Cleaning, Imputation)를 수행합니다.

    Key Logic:
        1. Efficiency: PyArrow 백엔드 사용으로 메모리 최적화.
        2. Integrity: KNN 보간법을 통한 결측치 처리.
        3. Business Logic: Data Leakage 유발 컬럼 제거.

    Returns:
        bool: 전처리 프로세스 성공 여부
    """
    logger.info("🔄 [Transform] 데이터 전처리 프로세스 시작...")

    file_path = "data/raw/challenger_data.json"
    
    if not os.path.exists(file_path):
        logger.error(f"❌ [Transform] 파일이 없습니다: {file_path}")
        return False

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        if 'entries' not in raw_data:
            logger.error("❌ [Transform] JSON 구조 오류 ('entries' 키 없음)")
            return False
        
        # ---------------------------------------------------------
        # 1. [Efficiency] PyArrow Backend 도입
        # ---------------------------------------------------------
        df = pd.DataFrame(raw_data['entries'])
        
        try:
            df = df.convert_dtypes(dtype_backend="pyarrow")
            logger.info("✅ [Efficiency] PyArrow Backend 적용 완료 (Memory Optimization)")
        except Exception as e:
            logger.warning(f"⚠️ PyArrow 변환 실패 (NumPy 사용): {e}")

        # ---------------------------------------------------------
        # 2. [Business Logic] Data Leakage 제거
        # ---------------------------------------------------------
        if 'gold_earned' in df.columns:
            df = df.drop(columns=['gold_earned'])
            logger.info("⚠️ [Integrity] Data Leakage 방지: 'gold_earned' 컬럼 삭제")

        # ---------------------------------------------------------
        # 3. [Data Integrity] KNN 기반 결측치 처리
        # ---------------------------------------------------------
        # 단순 평균(Mean) 대치는 티어 간 실력 격차를 무시하므로, 유사 유저 그룹(K=5) 기반 보간 사용.
        numeric_cols = df.select_dtypes(include=['int64', 'float64', 'Int64', 'Float64']).columns
        
        if len(numeric_cols) > 0:
            imputer = KNNImputer(n_neighbors=5)
            
            df_numeric = df[numeric_cols].to_numpy()
            imputed_data = imputer.fit_transform(df_numeric)
            
            df[numeric_cols] = imputed_data
            logger.info(f"✅ [Integrity] {len(numeric_cols)}개 컬럼에 대해 KNN 결측치 보간 완료")
        else:
            logger.warning("⚠️ 수치형 컬럼 부재로 KNN 건너뜀")

        # 4. 저장 (Processed Layer)
        os.makedirs("data/processed", exist_ok=True)
        save_path = "data/processed/cleaned_data.csv"
        df.to_csv(save_path, index=False)
        
        logger.info(f"✅ [Transform] 전처리 완료 및 저장: {save_path}")
        return True

    except Exception as e:
        logger.exception(f"❌ [Transform] 처리 중 치명적 오류: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_data()