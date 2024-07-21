{{ config(
  materialized = 'table'
)}}

with 

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),
drb_avg_weekly as ( select * from {{ ref('stg_avg_weekly_G_rate_b') }}),


drb_G as (select *
          ,EXTRACT(YEAR FROM TO_DATE("Miladi", 'DD/MM/YYYY')) as "G_Year_N"
          ,EXTRACT(DOW FROM TO_DATE("Miladi", 'DD/MM/YYYY')) as "G_WeekDay_N"
          ,"M_Week_N" AS "G_Week_N"
          from drb_
          ),

drb_join_weekly_avg as (select * from drb_G left join drb_avg_weekly using(asset, "G_Year_N", "G_Week_N")),

drb_diff_avg as (

        select 
            asset
            ,date
            ,"M_Week_N"
            ,"G_WeekDay_N"
            ,buy
            ,sell
            ,avg_buy
            ,avg_sell
            ,(buy- avg_buy) as diff_buy
            ,(sell - avg_sell) as diff_sell

            from drb_join_weekly_avg
)

select * from drb_diff_avg


