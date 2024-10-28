{{ config(
  materialized = 'table'
)}}

with

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),


drb_filtered as (
    select 
    date
    ,asset
    ,sell as r_price

    from drb_
    where date >= current_date - interval '11 days'
),

drb_lagged AS (
    SELECT 
        *
        ,LAG(r_price) OVER (PARTITION BY asset ORDER BY date) AS r_pre_price
    FROM drb_filtered
),

drb_change as (
    SELECT 
        *
        ,CASE 
        WHEN r_pre_price IS NOT NULL THEN 
            ((r_price::FLOAT - r_pre_price::FLOAT) / r_pre_price::FLOAT) * 100
        ELSE 
            NULL 
        END AS r_change_pr
FROM 
    drb_lagged
ORDER BY 
    asset, date
),

drb_sign as ( 

    SELECT 
        *
        ,CASE 
            WHEN r_change_pr > 0 THEN 'p'
            WHEN r_change_pr < 0 THEN 'n'
            ELSE 'z'
        END AS r_sign
    FROM 
        drb_change
),
drb_last10day as (


    select *
    from drb_sign
    where date >= current_date - interval '10 days'

)

select *
from drb_last10day