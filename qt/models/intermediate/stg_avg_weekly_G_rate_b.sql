{{ config(
  materialized = 'table'
)}}

with 

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),


drb_y as (
    select 
        asset
        ,EXTRACT(YEAR FROM TO_DATE("Miladi", 'DD/MM/YYYY')) as "G_Year_N"
        ,"M_Week_N" as "G_Week_N"
        ,buy
        ,sell
    from drb_
),

drb_weeklyaverage as (
    select   asset , "G_Year_N", "G_Week_N",  ROUND(avg(buy), 2)  as AVG_BUY , ROUND(avg(sell), 2) as AVG_SELL  from drb_y
    group by asset , "G_Year_N", "G_Week_N"
    order by asset , "G_Year_N", "G_Week_N"
    )

select * from drb_weeklyaverage