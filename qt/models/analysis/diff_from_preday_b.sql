{{ config(
  materialized = 'table'
)}}



WITH 

lst_7 AS (
    SELECT * FROM {{ ref('stg_last_7days_daily_rate_b')}}
),

RankedSells AS (
    SELECT
        "date",
        "asset",
        "sell",
        ROW_NUMBER() OVER (PARTITION BY "asset" ORDER BY "date" DESC) AS rn
    FROM lst_7
),
LastAndPreLast AS (
    SELECT
        a."asset",
        a."date" AS "last_day",
        b."date" AS "pre_last_day",
        a."sell" AS "last_day_sell",
        b."sell" AS "pre_last_day_sell"
    FROM
        RankedSells a
        JOIN RankedSells b ON a."asset" = b."asset" AND a.rn = 1 AND b.rn = 2
)
SELECT
    "asset"
    ,"last_day"
    ,"pre_last_day"
    ,"last_day_sell"
    ,"pre_last_day_sell"
    ,((last_day_sell - pre_last_day_sell) / pre_last_day_sell * 100) AS "pd"
FROM
    LastAndPreLast