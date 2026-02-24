{{ config(materialized='table') }}

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