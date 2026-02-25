{{ config(materialized='view') }}

with base as (
  select *
  from {{ ref('stg_reviews') }}
),
ranked as (
  select
    *,
    row_number() over (
      partition by review_id
      order by review_ts desc
    ) as rn
  from base
)
select
  review_id,
  user_id,
  business_id,
  stars,
  useful,
  funny,
  cool,
  text,
  review_ts,
  review_year
from ranked
where rn = 1