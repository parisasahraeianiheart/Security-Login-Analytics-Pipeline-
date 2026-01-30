# 🔐 Security Login Analytics Platform

An end-to-end **data engineering + analytics engineering + ML-ready platform** for security login monitoring, risk analytics, and suspicious activity detection.

This project simulates a real-world enterprise security analytics system and demonstrates:

- ETL pipelines  
- Analytics engineering  
- Data quality enforcement  
- Warehouse modeling  
- KPI marts  
- SQL analytics  
- ML-ready datasets  
- Interactive dashboards  

> This is not a notebook demo — it is a **mini data platform**.

---

## 🧠 Architecture Overview
Data Generation
↓
ETL Pipeline
↓
Parquet Lake
↓
DuckDB Warehouse
↓
Staging Layer
↓
Analytics Layer (dims/facts/marts)
↓
Quality Checks
↓
Streamlit Dashboard
↓
ML / Analytics / Decision Systems

---

## 🏗 Project Structure

security-login-analytics/
│
├── src/
│   ├── config.py                # Global config, paths, DB
│   ├── utils_logging.py         # Logging system
│   ├── utils_quality.py         # Data quality checks
│   ├── generate_data.py         # Synthetic data generator
│   ├── etl_extract.py           # Extract layer
│   ├── etl_transform.py         # Transform layer
│   ├── etl_load.py              # Load into DuckDB
│   ├── analytics_build.py       # Analytics models (dims/facts/marts)
│   ├── analytics_checks.py      # dbt-like data tests
│   ├── dashboard_app.py         # Streamlit dashboard
│
├── data/
│   ├── raw/                     # Raw files
│   ├── staging/                 # Cleaned parquet files
│
├── warehouse/
│   └── security.duckdb          # Analytics warehouse
│
└── README.md




👤 Author

Parisa Sahraeian
Data Scientist | Data Engineer | AI Systems Builder
