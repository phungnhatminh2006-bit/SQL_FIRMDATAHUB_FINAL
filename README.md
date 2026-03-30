# SQL_FIRMDATAHUB

## Group member
| Name | Student ID | Role & Responsibilities | Contribution |
|------|------------|-------------------------|--------------|
| **Vu Tran Cat Linh** | 11245899 | **Team Leader:**  |  |
| **Phung Nhat Minh** | 11245910 |  |  |
| **Tran Viet Long** |  |  |  |
| **Ngo Manh Duy** |  |  |  |

## 1. Project Overview

This project develops a Firm Data Hub, a centralized system designed to store, validate, version, and export panel data of Vietnamese listed firms for the period 2020–2024.

The system manages:
- Firm master data (ticker, company name, exchange, industry)
- Panel dataset with 38 variables per firm-year
- Data quality validation (QC)
- Snapshot-based version control
- Clean dataset extraction for analysis

## 2. Objectives
The main objectives of this project are:
- Design a relational database using DIM–FACT–SNAPSHOT architecture
- Build ETL pipelines to import and process firm-level data
- Implement data quality checks (QC)
- Enable version control via snapshot mechanism
- Export a clean and analysis-ready panel dataset

## Project Structure
```text
TEAM_<>_FirmDataHub/
│
├── sql/
│   └── schema_and_seed.sql
│
├── etl/
│   ├── import_firms.py
│   ├── create_snapshot.py
│   ├── import_panel.py
│   ├── qc_checks.py
│   └── export_panel.py
│
├── data/
│   ├── team_tickers.csv
│   ├── firms.xlsx
│   └── panel_2020_2024.xlsx
│
├── outputs/
│   ├── qc_report.csv
│   └── panel_latest.csv
│
└── README.md
```
## Dataset Description
- Number of firms: 20
- Time period: 2020–2024
- Total observations: 100 firm-year records
- Number of variables: 

## Data Pipeline
- **Step 1:** Import firm master data
- **Step 2:** Create snapshot
- **Step 3:** Import panel data
- **Step 4:** Run data quality check
- **Step 5:** Export clean dataset

## Data Quality Checks (QC)
The system implements the following validation rules:
- Ownership ratios must be within [0, 1]
- Shares outstanding must be greater than 0
- Total assets must be non-negative
- Current liabilities must be non-negative
- Growth ratio must fall within a configurable range (e.g., [-0.95, 5.0])
- Market value consistency check: market_value ≈ shares_outstanding × share_price

**All validation errors are recorded in qc_report.csv**

## Output files
