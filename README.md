# Yelp Modern Data Pipeline (DuckDB + dbt)

![CI](https://github.com/vpsn99/yelp-modern-data-pipeline/actions/workflows/ci.yml/badge.svg)

An end-to-end modern data pipeline built on the Yelp Open Dataset (~7M reviews, ~5GB raw JSON).

This project demonstrates production-style data engineering patterns using fully local, open-source tools including DuckDB, dbt, Prefect orchestration, and GitHub Actions CI.

---

## What This Project Demonstrates

- Memory-efficient ingestion of multi-GB JSON data
- Partitioned Parquet data lake design
- Columnar storage with ZSTD compression
- DuckDB analytical warehouse layer
- dbt staging, intermediate, and mart layers
- Data quality testing (not_null, unique, composite uniqueness)
- Incremental model strategy with rolling reprocess window
- Prefect orchestration of ingestion and transformation
- Automated validation via GitHub Actions CI

---

## Architecture Overview

Yelp JSON (5GB)  
↓  
Streaming ingestion (Python + Polars)  
↓  
Partitioned Parquet (review_year=YYYY)  
↓  
DuckDB warehouse  
↓  
dbt transformations (staging → intermediate → marts)  
↓  
Prefect orchestration (ingest → dbt run → dbt test)  
↓  
GitHub Actions CI (automated validation on push)  

---

## Project Structure

```
yelp-modern-data-pipeline/
│
├── pipelines/
│   └── ingest/
│
├── flows/
│   └── yelp_pipeline_flow.py
│
├── warehouse/
│   └── dbt/
│       └── yelp_dbt/
│
├── scripts/
│   └── run_all.ps1
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   ├── raw/        # Not committed
│   ├── staged/     # Not committed
│   └── duckdb/     # Not committed
│
└── README.md
```

---

## Dataset

Source: Yelp Open Dataset  
https://business.yelp.com/data/resources/open-dataset/

Download and place files under:

```
data/raw/
```

Required files:

- yelp_academic_dataset_review.json
- yelp_academic_dataset_business.json
- yelp_academic_dataset_user.json

---

## Local Setup

### 1. Create Virtual Environment

```
python -m venv .venv
.venv\Scripts\activate
pip install polars pyarrow duckdb dbt-duckdb prefect rich
```

---

### 2. Initial Staging (JSON → Parquet)

```
python pipelines/ingest/stage_yelp_json_to_parquet.py
```

---

### 3. Run dbt Transformations

```
cd warehouse/dbt/yelp_dbt
dbt run --profiles-dir ..
dbt test --profiles-dir ..
```

---

## Orchestration with Prefect

The pipeline is orchestrated using Prefect. The flow executes:

1. Incremental ingestion  
2. dbt run  
3. dbt test  

Run the full pipeline:

```
python flows/yelp_pipeline_flow.py
```

Each task is logged independently, and failures halt downstream execution.

---

## Continuous Integration (GitHub Actions)

A GitHub Actions workflow located at:

```
.github/workflows/ci.yml
```

runs automatically on every push.

The CI pipeline:

- Installs dependencies  
- Executes dbt run  
- Executes dbt test  
- Fails the build if data quality tests fail  

This ensures transformation logic remains reproducible and validated across changes.

---

## Incremental Model Strategy

`mart_business_monthly_ratings` is implemented as an incremental dbt model.

Key characteristics:

- materialized='incremental'  
- Composite unique key (business_id, review_month)  
- delete+insert strategy  
- Rolling reprocess window (default: 3 months)  

Override reprocess window:

```
dbt run --profiles-dir .. --select mart_business_monthly_ratings --vars "{reprocess_months: 12}"
```

---

## Data Quality

- not_null tests  
- uniqueness tests  
- Composite uniqueness via dbt_utils  
- Intermediate-layer deduplication  

All tests run locally and in CI.

---

## Example Mart

`mart_business_monthly_ratings` provides:

- Monthly review count  
- Average rating  
- Vote aggregates  
- Business metadata enrichment  

Designed to resemble a production analytics-ready reporting table.

---

## Future Enhancements

- Optional Snowflake backend  
- Object storage simulation  
- dbt documentation site  
- Containerized execution (Docker)  

---

## Author

Virendra Pratap Singh  
Senior Data Architect | Data Engineering | Analytics Platforms  
https://www.linkedin.com/in/virendra-pratap-singh-iitg/
