{{
  config(
    materialized='incremental',
    unique_key=['business_id', 'review_month'],
    incremental_strategy='delete+insert'
  )
}}

{#
  Reprocess window (months).
  Default = 3. Override with: --vars '{reprocess_months: 6}'
#}
{% set reprocess_months = var('reprocess_months', 3) %}

with reviews as (

    select
        business_id,
        date_trunc('month', review_ts) as review_month,
        count(*) as review_cnt,
        avg(stars) as avg_stars,
        sum(useful) as useful_votes,
        sum(funny) as funny_votes,
        sum(cool) as cool_votes
    from {{ ref('stg_reviews') }}

    {% if is_incremental() %}
      where review_ts >= date_trunc('month', current_timestamp - interval '{{ reprocess_months }} months')
    {% endif %}

    group by 1, 2
),

business as (
    select
        business_id,
        name,
        city,
        state,
        categories,
        is_open
    from {{ ref('stg_business') }}
)

select
    r.business_id,
    b.name,
    b.city,
    b.state,
    b.is_open,
    b.categories,
    r.review_month,
    r.review_cnt,
    round(r.avg_stars, 3) as avg_stars,
    r.useful_votes,
    r.funny_votes,
    r.cool_votes
from reviews r
join business b using (business_id)