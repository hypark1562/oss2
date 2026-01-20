import pandas as pd
import numpy as np
import os
import json # <--- json 모듈 추가
from sklearn.impute import KNNImputer

def process_data():
    print("🔄 데이터 전처리 시작...")

    # 1. JSON 파일 구조에 맞게 읽기 (수정된 부분)
    # 그냥 read_json을 쓰면 안 되고, 'entries' 리스트를 꺼내야 함
    file_path = "data/raw/challenger_data.json"
    
    if not os.path.exists(file_path):
        print(f"❌ 파일이 없습니다: {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    # 'entries' 키 안에 실제 유저 데이터가 들어있음
    if 'entries' in raw_data:
        df = pd.DataFrame(raw_data['entries'])
    else:
        print("❌ JSON 구조가 예상과 다릅니다 ('entries' 키 없음)")
        return

    # 2. [비즈니스 로직] Data Leakage 제거
    # (gold_earned 컬럼이 있다면 삭제 - 챌린저 데이터엔 없을 수도 있음)
    if 'gold_earned' in df.columns:
        df = df.drop(columns=['gold_earned'])
        print("⚠️ Data Leakage 방지를 위해 'gold_earned' 컬럼 삭제함")

    # 3. [공학적 로직] 결측치 처리 (KNN)
    # 수치형 컬럼만 선택 (wins, losses, leaguePoints 등)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    # ★ 에러 방지용 안전장치 추가 ★
    if len(numeric_cols) > 0:
        imputer = KNNImputer(n_neighbors=5)
        df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        print(f"✅ {len(numeric_cols)}개 컬럼에 대해 KNN 결측치 보간 완료")
    else:
        print("⚠️ 수치형 컬럼을 찾을 수 없어 KNN 건너뜀")

    # 4. 저장
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/cleaned_data.csv", index=False)
    print("✅ 데이터 저장 완료: data/processed/cleaned_data.csv")

if __name__ == "__main__":
    process_data()