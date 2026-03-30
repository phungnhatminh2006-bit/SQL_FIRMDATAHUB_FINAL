# SQL_FIRMDATAHUB - TEAM 11

## Group member
| Name | Student ID | Role & Responsibilities | Contribution |
|------|------------|-------------------------|--------------|
| **Vu Tran Cat Linh** | 11245899 | **Team Leader**  |  |
| **Phung Nhat Minh** | 11245910 |  |  |
| **Tran Viet Long** | 11245868 |  |  |
| **Ngo Manh Duy** | 11245901 |  |  |

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

## 3. System Architecture
The database is structured as follows:
Dimension Tables (DIM)
- `dim_firm`
- `dim_exchange`
- `dim_industry_l2`
- `dim_data_source`

Fact Tables (FACT)
- `fact_ownership_year`
- `fact_financial_year`
- `fact_cashflow_year`
- `fact_market_year`
- `fact_innovation_year`
- `fact_firm_year_meta`

Snapshot Versioning
- `fact_data_snapshot`

Audit
- `fact_value_override_log`

View
- `vw_firm_panel_latest (final dataset for analysis)`
**All fact tables use composite keys (firm_id, fiscal_year, snapshot_id) to ensure version control and data consistency.

## 4. Project Structure
```text
TEAM_11_FirmDataHub/
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
## 5. Dataset Description
- Number of firms: 20
- Time period: 2020–2024
- Total observations: 100 firm-year records
- Number of variables: 

## 6. Data Pipeline
- **Step 1:** Import firm master data
- **Step 2:** Create snapshot
- **Step 3:** Import panel data
- **Step 4:** Run data quality check
- **Step 5:** Export clean dataset

## 7. Data Quality Checks (QC)
The system implements the following validation rules:
- Ownership ratios must be within [0, 1]
- Shares outstanding must be greater than 0
- Total assets must be non-negative
- Current liabilities must be non-negative
- Growth ratio must fall within a configurable range (e.g., [-0.95, 5.0])
- Market value consistency check: market_value ≈ shares_outstanding × share_price

**All validation errors are recorded in `qc_report.csv`**

## 8. Output Dataset
`panel_latest.csv`

Contains:
- ticker
- fiscal_year
- 38 variables

This dataset is extracted from the latest snapshot per firm-year, ensuring consistency and version control.

## 9. Evaluation Coverage
This project satisfies all requirements:
- Database schema with constraints and views
- ETL pipeline (firms + panel)
- Snapshot versioning
- Data quality checks
- Clean dataset export
- Reproducibility
