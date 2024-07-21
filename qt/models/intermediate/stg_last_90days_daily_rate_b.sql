{{ config(
  materialized = 'table'
)}}

with

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),


filtered_data as (
    select *
    from drb_
    where date >= current_date - interval '90 days'
)

select *
from filtered_data