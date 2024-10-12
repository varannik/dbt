{{ config(
  materialized = 'table'
)}}

with
cl_ranges as (select * from {{ref('stg_range_date_calendar')}}),
rates as ( select * from {{ref('stg_daily_rate_b')}}),

periods as (
    select 
      G_Date_Ref
      ,range_data
      ,asset
      ,sell
      ,buy
      from cl_ranges
      inner join rates
      on rates.date = cl_ranges.g_date_ref
)
select * from periods