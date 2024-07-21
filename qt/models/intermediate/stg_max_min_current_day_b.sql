{{ config(
  materialized = 'table'
)}}

with

xe_realtime as ( select * from {{source('raw', 'realtime_bonbast')}}),


xe_filtered as (
    SELECT
        date,
        asset,
        sell,
        buy,
        MAX(date) OVER (PARTITION BY asset) AS last_date
    FROM xe_realtime
),

xe_MaxMin_lastday as ( 

        SELECT
            asset,
            date,
            MAX(sell) AS max_sell,
            MIN(sell) AS min_sell,
            MAX(buy) AS max_buy,
            MIN(buy) AS min_buy
        FROM
            xe_filtered
        WHERE
            date = last_date
        GROUP BY
            asset, date
)

select *
from xe_MaxMin_lastday


