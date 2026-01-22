# LoL Challenger Data Pipeline

[Python 3.9+] [Code Style: Black] [License: MIT]

An End-to-End data pipeline that collects, transforms, loads, and visualizes League of Legends Challenger tier data using the Riot API.

---

## Architecture Overview

```mermaid
graph LR
    %% 노드 정의
    A[Riot API]
    B(Raw Data <br/> JSON)
    C{Data Processing <br/> Pandas/PyArrow}
    D[(SQLite DB)]
    E[Streamlit <br/> Dashboard]

    %% 데이터 흐름
    A -->|Extract| B
    B -->|Transform| C
    C -->|Load| D
    C -->|Visualize| E

    %% CI/CD 파이프라인 표시
    subgraph DevOps [Automated Pipeline]
        F[GitHub Actions] -.->|Test & Lint| C
        G[Docker] -.->|Build| E
    end

    %% 스타일링 (색상 입히기)
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#bbf,stroke:#333,stroke-width:2px
    style D fill:#ff9,stroke:#333,stroke-width:2px
    style E fill:#9f9,stroke:#333,stroke-width:2px
```

---

## Key Features

- Automated ETL Pipeline: Full automation using Python scripts.
- Enterprise Logging: Implemented RotatingFileHandler for log management.
- Data Integrity: KNN Imputation for missing values and leakage prevention.
- DevOps Standards: Makefile for build automation and pre-commit hooks.
- Config Management: Centralized YAML configuration.

---

## Quick Start

### 1. Installation
Run the following command to install dependencies:
$ make install

### 2. Configuration
Create a .env file and add your Riot API key:
RIOT_API_KEY=your_api_key_here

### 3. Execution
Run the full ETL pipeline:
$ make run

### 4. Dashboard
Launch the analytics dashboard:
$ make dashboard

---

## Project Structure

The project follows a modular architecture designed for scalability and maintainability.

```text
lol-data-pipeline/
├── .github/workflows/   # CI/CD Pipeline Configuration (GitHub Actions)
├── docs/                # Documentation (ADR, ERD, Analysis Reports)
├── etl/                 # ETL Modules
│   ├── extract.py       # Riot API Data Fetcher
│   ├── transform.py     # Data Cleaning & Feature Engineering
│   └── load.py          # SQLite Database Loader
├── tests/               # Unit & Integration Tests (Pytest)
├── utils/               # Shared Utilities (Logger, Config Loader)
├── .gitignore           # Git Exclusion Rules
├── config.yaml          # Centralized Configuration
├── dashboard.py         # Streamlit Analytics Dashboard (Visualization)
├── Dockerfile           # Docker Container Definition
├── main.py              # Pipeline Entry Point
├── Makefile             # Automation Commands (Make run, Make test)
├── README.md            # Project Overview & Documentation
└── requirements.txt     # Python Dependencies
```
![Dashboard Preview](dashboard_preview.png)
> **Snapshot:** Challenger Tier players' win-rate distribution and correlation analysis.

## Data Schema (ERD)

![ERD Structure](erd.png)
