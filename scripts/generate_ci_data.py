from pathlib import Path
from datetime import datetime
import polars as pl

ROOT = Path("ci_data")
STAGED = ROOT / "staged"
DUCKDB_DIR = ROOT / "duckdb"

def ensure_dirs():
    (STAGED / "reviews" / "review_year=2020").mkdir(parents=True, exist_ok=True)
    (STAGED / "business").mkdir(parents=True, exist_ok=True)
    (STAGED / "users").mkdir(parents=True, exist_ok=True)
    DUCKDB_DIR.mkdir(parents=True, exist_ok=True)

def main():
    ensure_dirs()

    # business
    business = pl.DataFrame([
        {
            "business_id": "b1", "name": "Cafe A", "address": "1 Main St",
            "city": "Montreal", "state": "QC", "postal_code": "H1H1H1",
            "latitude": 45.5, "longitude": -73.6, "categories": "Cafe,Food",
            "stars": 4.2, "review_count": 10, "is_open": 1,
        },
        {
            "business_id": "b2", "name": "Diner B", "address": "2 Main St",
            "city": "Ottawa", "state": "ON", "postal_code": "K1K1K1",
            "latitude": 45.4, "longitude": -75.7, "categories": "Diner,Food",
            "stars": 3.8, "review_count": 8, "is_open": 1,
        },
    ])
    business.write_parquet(STAGED / "business" / "business_part_ci.parquet", compression="zstd")

    # users
    users = pl.DataFrame([
        {"user_id": "u1", "name": "Alice", "yelping_since": "2019-01-01", "review_count": 5, "average_stars": 4.0, "fans": 0},
        {"user_id": "u2", "name": "Bob", "yelping_since": "2018-06-10", "review_count": 3, "average_stars": 3.7, "fans": 1},
    ])
    users.write_parquet(STAGED / "users" / "users_part_ci.parquet", compression="zstd")

    # reviews (include a deliberate duplicate review_id to test dedup later)
    reviews = pl.DataFrame([
        {"review_id": "r1", "user_id": "u1", "business_id": "b1", "stars": 5, "useful": 1, "funny": 0, "cool": 0, "text": "great", "date": "2020-02-01 10:00:00"},
        {"review_id": "r1", "user_id": "u1", "business_id": "b1", "stars": 4, "useful": 0, "funny": 0, "cool": 0, "text": "edit",  "date": "2020-02-05 10:00:00"},
        {"review_id": "r2", "user_id": "u2", "business_id": "b2", "stars": 3, "useful": 0, "funny": 1, "cool": 0, "text": "ok",    "date": "2020-03-01 12:00:00"},
    ])
    reviews.write_parquet(STAGED / "reviews" / "review_year=2020" / "reviews_part_ci.parquet", compression="zstd")

    print("CI data generated under ci_data/")

if __name__ == "__main__":
    main()