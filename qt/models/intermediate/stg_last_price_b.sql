{{ config(
  materialized = 'table'
)}}

with

xe_realtime as ( select * from {{source('raw', 'realtime_bonbast')}}),

most_recent_data as (
    SELECT date, asset, sell, buy
    FROM (
        SELECT
            date,
            asset,
            sell,
            buy,
            ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS rn
        FROM xe_realtime
    ) sub
    WHERE sub.rn = 1
)

select *
from most_recent_data
where asset NOT IN (
'½ Azadi'
,'¼ Azadi'
,'Azadi'
,'Emami'
,'Gerami'
,'Turkmenistani Manat'
,'Georgian Lari'
,'Kyrgystani Som'
,'Tajikistani Somoni'
,'South Korean won'
,'New Zealand Dollar'
,'Pakistani Rupee'
,'Syrian Pound'
 )



