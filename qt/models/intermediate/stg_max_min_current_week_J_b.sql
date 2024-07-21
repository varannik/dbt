{{ config(
  materialized = 'table'
)}}

with

drb_ as (select * from {{ ref('stg_daily_rate_b') }}),

last_day as ( 
    select "J_Year_N","J_Week_N"
    from drb_
    order by "Jalali" desc 
    limit (1) 
),


xe_filtered as (
    SELECT
        date,
        asset,
        sell,
        buy
    FROM drb_
    INNER JOIN last_day using ( "J_Year_N","J_Week_N")

),

xe_MaxMin_lastweek as ( 

    SELECT
        asset,
        MAX(sell) AS max_sell,
        MIN(sell) AS min_sell,
        MAX(buy) AS max_buy,
        MIN(buy) AS min_buy
    FROM
        xe_filtered
    GROUP BY
        asset
)

select *
from xe_MaxMin_lastweek
