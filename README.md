# Yelp Modern Data Pipeline (DuckDB + dbt)

An end-to-end modern data pipeline built on the Yelp Open Dataset (\~7M
reviews, \~5GB raw JSON).

This project demonstrates production-style data engineering patterns
using fully local, open-source tools.

------------------------------------------------------------------------

## What This Project Demonstrates

-   Streaming ingestion of multi-GB JSON safely (memory-efficient
    batching)
-   Partitioned Parquet data lake design
-   Columnar storage with ZSTD compression
-   DuckDB analytical warehouse layer
-   dbt staging models and marts
-   Data quality tests (not_null, unique)
-   Reproducible local execution

------------------------------------------------------------------------

## Architecture Overview

Yelp JSON (5GB) ↓ Streaming ingestion (Python + Polars) ↓ Partitioned
Parquet (review_year=YYYY) ↓ DuckDB (analytics engine) ↓ dbt (staging +
marts + tests)

------------------------------------------------------------------------

## Project Structure

    yelp-modern-data-pipeline/
    │
    ├── pipelines/
    │   └── ingest/
    │       ├── stage_yelp_json_to_parquet.py
    │       └── stage_yelp_json_to_parquet_small.py
    │
    ├── warehouse/
    │   └── dbt/
    │       └── yelp_dbt/
    │           └── models/
    │               ├── staging/
    │               └── marts/
    │
    ├── scripts/
    │   └── run_all.ps1
    │
    ├── data/
    │   ├── raw/        # Yelp JSON files (NOT committed)
    │   ├── staged/     # Parquet outputs (NOT committed)
    │   └── duckdb/     # Local DuckDB database (NOT committed)
    │
    └── README.md

------------------------------------------------------------------------

## Dataset

Source: Yelp Open Dataset\
https://business.yelp.com/data/resources/open-dataset/

Download manually and place extracted files under:

    data/raw/

Required files:

-   yelp_academic_dataset_review.json
-   yelp_academic_dataset_business.json
-   yelp_academic_dataset_user.json

------------------------------------------------------------------------

## Local Setup

### 1️. Create Virtual Environment

    python -m venv .venv
    .venv\Scripts\activate
    pip install polars pyarrow duckdb dbt-duckdb rich

------------------------------------------------------------------------

### 2️. Stage Raw Data (JSON → Parquet)

    python pipelines/ingest/stage_yelp_json_to_parquet.py
    python pipelines/ingest/stage_yelp_json_to_parquet_small.py

------------------------------------------------------------------------

### 3️. Run dbt Models

    cd warehouse/dbt/yelp_dbt
    dbt run --profiles-dir ..
    dbt test --profiles-dir ..

------------------------------------------------------------------------

### Run Everything (PowerShell)

    powershell -ExecutionPolicy Bypass -File scripts/run_all.ps1

------------------------------------------------------------------------

## Key Engineering Decisions

-   Chunked ingestion (250k records per batch)
-   Explicit partitioning strategy by review_year
-   Separation of ingestion and transformation layers
-   dbt-based transformations for maintainability
-   Data quality tests embedded in pipeline

------------------------------------------------------------------------

## Example Mart

`mart_business_monthly_ratings`

Provides:

-   Monthly review count
-   Average star rating
-   Vote aggregates
-   Business metadata enrichment

------------------------------------------------------------------------

## Future Enhancements

-   Incremental model builds
-   Orchestration with Prefect
-   GitHub Actions CI
-   Optional Snowflake backend

------------------------------------------------------------------------

## Author

Virendra Pratap Singh\
Senior Data Architect \| Data Engineering \| Analytics Platforms
