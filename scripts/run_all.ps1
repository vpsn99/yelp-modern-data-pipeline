Write-Host "===== Yelp Modern Data Pipeline ====="

Write-Host "1) Staging reviews..."
python pipelines/ingest/stage_yelp_json_to_parquet.py

Write-Host "2) Staging business + users..."
python pipelines/ingest/stage_yelp_json_to_parquet_small.py

Write-Host "3) Running dbt models..."
cd warehouse/dbt/yelp_dbt
dbt run --profiles-dir ..

Write-Host "4) Running dbt tests..."
dbt test --profiles-dir ..

Write-Host "Pipeline completed successfully."