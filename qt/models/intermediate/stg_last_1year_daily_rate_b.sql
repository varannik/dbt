{{ config(
  materialized = 'table'
)}}

with

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),


filtered_data as (
    select 
    date
    ,asset
    ,sell
    from drb_
    where date >= current_date - interval '365 days'
)

select *
from filtered_data