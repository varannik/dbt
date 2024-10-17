{{ config(
  materialized = 'table'
)}}

with

rsi_columns as (
    SELECT 
      date
      ,asset
      ,rsi_14_days

  FROM {{ref('stg_daily_rate_features_base_b')}}

),

rsi_sort as (
    SELECT *,
         ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS rank
    FROM rsi_columns
),

rsi_14days as (
    SELECT *
    FROM rsi_sort
    WHERE rank <= 14
    ORDER BY asset , date 
)

select * from rsi_14days
