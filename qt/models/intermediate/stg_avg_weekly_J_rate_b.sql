{{ config(
  materialized = 'table'
)}}

with 

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),

drb_weeklyaverage as (
    select   asset , "J_Year_N", "J_Week_N",  ROUND(avg(buy::numeric::integer), 2)  as AVG_BUY , ROUND(avg(sell::numeric::integer), 2) as AVG_SELL  from drb_
    group by asset , "J_Year_N", "J_Week_N"
    order by asset , "J_Year_N", "J_Week_N"
    )

select * from drb_weeklyaverage




