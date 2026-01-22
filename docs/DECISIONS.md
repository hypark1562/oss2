# Architecture Decision Records (ADR)

## 1. Database Selection: SQLite
- **Context**: The project requires a lightweight, zero-configuration database for local development and batch processing.
- **Decision**: Adopt **SQLite** as the primary data store.
- **Justification**:
    - Zero setup required (serverless).
    - Native support in Python standard library.
    - Sufficient performance for single-writer batch pipelines.
- **Alternatives Considered**: PostgreSQL (rejected due to infrastructure overhead for this project scale).

## 2. Data Integrity & Leakage Prevention
- **Context**: Preliminary analysis revealed certain features (e.g., `gold_earned`) had unrealistic correlations with the target variable.
- **Decision**: Remove `gold_earned` and `total_damage` columns during the transformation phase.
- **Evidence**: See `leakage_report.md` for detailed correlation analysis and feature importance scores.

## 3. DevOps Strategy
- **Context**: Need to ensure code consistency across different development environments.
- **Decision**: Implement a containerized workflow using **Docker** and automated CI via **GitHub Actions**.
- **Consequences**:
    - Ensures "Build Once, Run Anywhere".
    - Enforces code quality standards (Linting/Testing) before merging.
