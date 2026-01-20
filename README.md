# 📉 LoL Data ETL Pipeline & Cost Optimization (Baseline)

### 📌 Project Overview
- **Objective:** 라이엇(Riot) API 기반의 데이터 파이프라인 구축 및 데이터 무결성 확보
- **Tech Stack:** Python, Pandas(PyArrow), SQLAlchemy, SQLite, Scikit-learn
- **Role:** Data Engineering & Preprocessing Logic Design

### 🛠️ Key Engineering Decisions (합격 포인트)
1.  **PyArrow Backend 도입:**
    - 기존 NumPy 기반 Pandas 대비 메모리 사용량 최적화를 위해 `pyarrow` 엔진 적용.
    - 대용량 게임 데이터 처리 시 병목 현상 해결 목적.

2.  **Logic-based Imputation (KNN):**
    - 결측치를 단순 평균(Mean)으로 대체할 경우, 티어(Tier) 간 실력 격차가 무시되는 데이터 왜곡 발생.
    - 이를 방지하기 위해 **KNN Imputation**을 적용하여 가장 유사한 유저 그룹의 스탯으로 보간함.

3.  **Data Leakage Prevention:**
    - 게임 승패(`win`) 예측 시, 게임 종료 후 확정되는 사후 데이터(예: `gold_earned`)를 학습에서 배제하여 모델 신뢰성 확보.

### ⚠️ Limitations
본 프로젝트는 로직 검증이 목적입니다. 실제 비즈니스 환경 적용 시 발생할 수 있는 **API Rate Limit, 데이터 편향(Bias)**을 인지하고 있습니다.