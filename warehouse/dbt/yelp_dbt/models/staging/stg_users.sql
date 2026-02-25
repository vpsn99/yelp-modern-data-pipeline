with src as (
  select *
  from read_parquet('{{ var("data_root") }}/staged/users/*.parquet')
)
select
  user_id,
  name,
  yelping_since,
  cast(review_count as bigint) as review_count,
  cast(average_stars as double) as average_stars,
  cast(fans as bigint) as fans
from src