{{ config(
  materialized = 'table'
)}}

WITH 

r_ as (select * from {{ ref('stg_last_10days_for_check_prediction_b') }}),
f_ as (select * from {{ ref('stg_trend_direction_change_ai_predictions') }}),

r_f_join as ( 
select * 
from f_
left join r_
using(date, asset)
),

r_f_true_prediction as (

select * 
, CASE WHEN r_sign = f_sign THEN 'T' ELSE 'F' END AS TP
from  r_f_join
),

r_f_rank as (

    SELECT *,
         ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS rank
    FROM r_f_true_prediction
),

r_f__filterd as (
  SELECT * 
  FROM r_f_rank
  WHERE rank <= 11
)
select * from r_f__filterd