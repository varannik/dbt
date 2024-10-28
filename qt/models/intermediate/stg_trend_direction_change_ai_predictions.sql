{{ config(
  materialized = 'table'
)}}
WITH 

ai_ as ( select * from {{source('raw', 'ai_predictions')}}),

ai_lagged AS (
    SELECT 
    date
    ,asset
    ,price as f_price
    ,LAG(price) OVER (PARTITION BY asset ORDER BY date) AS f_pre_price
    FROM ai_
),

ai_change as (
    SELECT 
        *
        ,CASE 
        WHEN f_pre_price IS NOT NULL THEN 
            ((f_price::FLOAT - f_pre_price::FLOAT) / f_pre_price::FLOAT) * 100
        ELSE 
            NULL 
        END AS f_change_pr
FROM 
    ai_lagged
ORDER BY 
    asset, date
),

ai_sign as ( 

    SELECT 
        *
        ,CASE 
            WHEN f_change_pr > 0 THEN 'p'
            WHEN f_change_pr < 0 THEN 'n'
            ELSE 'z'
        END AS f_sign
    FROM 
        ai_change
)

select * from ai_sign

