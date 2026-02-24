with src as (
  select *
  from read_parquet('{{ var("project_root") }}data/staged/business/*.parquet')
)
select
  business_id,
  name,
  address,
  city,
  state,
  postal_code,
  cast(latitude as double)    as latitude,
  cast(longitude as double)   as longitude,
  categories,
  cast(stars as double)       as stars,
  cast(review_count as bigint) as review_count,
  cast(is_open as integer)    as is_open
from src