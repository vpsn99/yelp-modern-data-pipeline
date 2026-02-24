with src as (
  select *
  from read_parquet(
    '{{ var("project_root") }}data/staged/reviews/review_year=*/**/*.parquet',
    hive_partitioning=1
  )
)
select
  review_id,
  user_id,
  business_id,
  cast(stars as double)           as stars,
  cast(useful as bigint)          as useful,
  cast(funny as bigint)           as funny,
  cast(cool as bigint)            as cool,
  text,
  cast(date as timestamp)         as review_ts,
  cast(review_year as integer)    as review_year
from src