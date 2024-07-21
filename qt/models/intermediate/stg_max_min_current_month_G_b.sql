{{ config(
  materialized = 'table'
)}}

with

drb_ as (select * from {{ ref('stg_daily_rate_b') }}),


xe_filtered as (
    SELECT
        date,
        asset,
        sell,
        buy
    FROM drb_
    WHERE date >= date_trunc('month', CURRENT_DATE)
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
