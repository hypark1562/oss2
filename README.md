![Python 3.10](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Build Status](https://github.com/hypark1562/oss2/actions/workflows/python-app.yml/badge.svg)

# 📉 LoL Data ETL Pipeline & Cost Optimization (Baseline)

### 📌 Project Overview
- **Objective:** 라이엇(Riot) API 기반의 데이터 파이프라인 구축 및 데이터 무결성 확보
- **Tech Stack:** Python, Pandas(PyArrow), SQLAlchemy, SQLite, Scikit-learn
- **Role:** Data Engineering & Preprocessing Logic Design

### 🛠️ Key Engineering Decisions 
1.  **PyArrow Backend 도입:**
    - 기존 NumPy 기반 Pandas 대비 메모리 사용량 최적화를 위해 `pyarrow` 엔진 적용.
    - 대용량 게임 데이터 처리 시 병목 현상 해결 목적.

2.  **Logic-based Imputation (KNN):**
    - 결측치를 단순 평균(Mean)으로 대체할 경우, 티어(Tier) 간 실력 격차가 무시되는 데이터 왜곡 발생.
    - 이를 방지하기 위해 **KNN Imputation**을 적용하여 가장 유사한 유저 그룹의 스탯으로 보간함.

3.  **Data Leakage Prevention:**
    - 게임 승패(`win`) 예측 시, 게임 종료 후 확정되는 사후 데이터(예: `gold_earned`)를 학습에서 배제하여 모델 신뢰성 확보.
## 🚀 Quick Start

**1. 환경 설정**
```bash
# 필수 라이브러리 설치
pip install -r requirements.txt

### 🗂️ Data Schema (ERD)
> **데이터 무결성(Integrity)을 위해 3개의 정규화된 테이블로 설계했습니다.**

![ERD Architecture](./data/schema/erd.png)

#### 📋 Table Description
1. **USERS (Dimension):** 소환사의 불변 정보 (Tier, Rank 등)를 관리합니다.
2. **MATCHES (Dimension):** 게임 자체의 메타 정보 (패치 버전, 게임 길이)를 담습니다.
3. **STATS (Fact):** 유저와 게임을 연결하는 핵심 플레이 로그 (KDA, 딜량, 승패)입니다.
   - *FK Constraint:* 존재하지 않는 유저나 게임 ID가 들어오지 못하도록 외래키 제약을 설정했습니다.

### ⚠️ Limitations
본 프로젝트는 로직 검증이 목적입니다. 실제 비즈니스 환경 적용 시 발생할 수 있는 **API Rate Limit, 데이터 편향(Bias)**을 인지하고 있습니다.