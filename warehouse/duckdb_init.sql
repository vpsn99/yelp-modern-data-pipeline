-- Creates a view over staged parquet (partitioned by review_year=YYYY folders)

CREATE OR REPLACE VIEW stg_reviews AS
SELECT
    review_id,
    user_id,
    business_id,
    stars,
    useful,
    funny,
    cool,
    text,
    date::TIMESTAMP AS review_ts,
    review_year
FROM read_parquet('data/staged/reviews/review_year=*/**/*.parquet', hive_partitioning=1);

-- Quick sanity helpers
CREATE OR REPLACE VIEW stg_reviews_counts AS
SELECT
  review_year,
  count(*) AS cnt
FROM stg_reviews
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW stg_business AS
SELECT *
FROM read_parquet('data/staged/business/*.parquet');

CREATE OR REPLACE VIEW stg_users AS
SELECT *
FROM read_parquet('data/staged/users/*.parquet');

-- Reviews (partitioned parquet)
CREATE OR REPLACE VIEW stg_reviews AS
SELECT
    review_id,
    user_id,
    business_id,
    stars,
    useful,
    funny,
    cool,
    text,
    date::TIMESTAMP AS review_ts,
    review_year
FROM read_parquet('data/staged/reviews/review_year=*/**/*.parquet', hive_partitioning=1);

CREATE OR REPLACE VIEW stg_reviews_counts AS
SELECT
  review_year,
  count(*) AS cnt
FROM stg_reviews
GROUP BY 1
ORDER BY 1;

-- Business
CREATE OR REPLACE VIEW stg_business AS
SELECT *
FROM read_parquet('data/staged/business/*.parquet');

-- Users
CREATE OR REPLACE VIEW stg_users AS
SELECT *
FROM read_parquet('data/staged/users/*.parquet');