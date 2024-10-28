{{ config(
  materialized = 'table'
)}}

with

f as (SELECT * FROM {{ref('stg_daily_rate_features_base_b')}}),

f_d as (SELECT * FROM f where asset = 'US Dollar'),

f_d_rank as (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY date) AS row_num,
        COUNT(*) OVER () AS total_rows
    FROM f_d
),

f_d_rank_removenullrow as ( 

  SELECT
      *
  FROM f_d_rank
  WHERE row_num > 31
)


select * from f_d_rank_removenullrow
