{{ config(
  materialized = 'table'
)}}

with 

diff_avg_G as ( select * from {{ ref('stg_sum_diff_per_weeks_G_b')}}),
diff_avg_J as ( select * from {{ ref('stg_sum_diff_per_weeks_J_b')}}),

uni_diff_avg as ( 

    select 
        asset
        ,"G_WeekDay_N" as weekday_num
        ,week_number as past_week_range
        -- ,sum_diff_buy
        -- ,sum_diff_sell
        ,avg_diff_per_buy
        ,avg_diff_per_sell
        ,'G' as cal_type
    from 
        diff_avg_G
    
    union all

    select 
        asset
        ,"J_WeekDay_N"-1 as weekday_num -- Persian calandar weekdays start at 1 and for match with Ge calendar week days that start from 0 
        ,week_number as past_week_range
        -- ,sum_diff_buy
        -- ,sum_diff_sell
        ,avg_diff_per_buy
        ,avg_diff_per_sell
        ,'J' as cal_type
    from 
        diff_avg_J


)

select * from uni_diff_avg

