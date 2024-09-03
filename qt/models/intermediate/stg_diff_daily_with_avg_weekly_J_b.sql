{{ config(
  materialized = 'table'
)}}

with 

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),
drb_avg_weekly as ( select * from {{ ref('stg_avg_weekly_J_rate_b') }}),

drb_join_weekly_avg as (select * from drb_ left join drb_avg_weekly using(asset, "J_Year_N", "J_Week_N")),

drb_diff_avg as (

        select 
            asset
            ,date
            ,"J_Week_N"
            ,"J_WeekDay_N"
            ,buy
            ,sell
            ,avg_buy
            ,avg_sell
            ,(buy- avg_buy) as diff_buy
            ,(sell - avg_sell) as diff_sell
            ,(buy- avg_buy)/avg_buy as diff_per_buy
            ,(sell - avg_sell)/avg_sell as diff_per_sell

            from drb_join_weekly_avg
)

select * from drb_diff_avg

