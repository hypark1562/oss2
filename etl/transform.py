import pandas as pd
import numpy as np
import os
import json
from sklearn.impute import KNNImputer

def process_data():
    print("🔄 데이터 전처리 시작...")

   
    file_path = "data/raw/challenger_data.json"
    
    if not os.path.exists(file_path):
        print(f"❌ 파일이 없습니다: {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    if 'entries' in raw_data:
        # ------------------------------------------------------------------
        # [Critical Fix] README와의 정합성을 위한 PyArrow 변환
        # ------------------------------------------------------------------
        
        df = pd.DataFrame(raw_data['entries'])
        
        
        try:
            df = df.convert_dtypes(dtype_backend="pyarrow")
            print("✅ PyArrow Backend 적용 완료 (Memory Optimization)")
        except Exception as e:
            print(f"⚠️ PyArrow 변환 실패 (기존 NumPy 사용): {e}")
            
    else:
        print("❌ JSON 구조가 예상과 다릅니다 ('entries' 키 없음)")
        return

    
    # 챌린저 유저 정보에는 골드 데이터가 없으나, 추후 Match 데이터 처리 시를 위한 로직임
    if 'gold_earned' in df.columns:
        df = df.drop(columns=['gold_earned'])
        print("⚠️ Data Leakage 방지를 위해 'gold_earned' 컬럼 삭제함")

    
    # PyArrow 타입에서는 select_dtypes(include=[np.number])가 안 먹힐 수 있음 -> 안전하게 처리
    numeric_cols = df.select_dtypes(include=['int64', 'float64', 'Int64', 'Float64']).columns
    
    if len(numeric_cols) > 0:
        # KNNImputer는 아직 PyArrow 타입을 완벽 지원하지 않을 수 있어 numpy로 변환 후 처리
        imputer = KNNImputer(n_neighbors=5)
        df_numeric = df[numeric_cols].to_numpy() # NumPy로 변환
        
        imputed_data = imputer.fit_transform(df_numeric)
        
        # 다시 DataFrame에 넣기
        df[numeric_cols] = imputed_data
        print(f"✅ {len(numeric_cols)}개 컬럼(Wins, Losses 등)에 대해 KNN 결측치 보간 완료")
    else:
        print("⚠️ 수치형 컬럼을 찾을 수 없어 KNN 건너뜀")

    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/cleaned_data.csv", index=False)
    print("✅ 데이터 저장 완료: data/processed/cleaned_data.csv")

if __name__ == "__main__":
    process_data()