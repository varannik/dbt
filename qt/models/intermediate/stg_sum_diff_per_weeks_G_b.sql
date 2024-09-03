{{ config(materialized='table') }}


{% set table = 'stg_diff_daily_with_avg_weekly_G_b' %}
{% set date_column = 'date' %}
{% set weeks_range = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233] %}

with weekly_sums as (

    {% for i in weeks_range %}

        SELECT 
            asset
            ,"G_WeekDay_N"
            ,{{ i }} as week_number
            ,SUM(diff_buy) as sum_diff_buy 
            ,SUM(diff_sell) as sum_diff_sell
            ,AVG(diff_per_buy) as avg_diff_per_buy
            ,AVG(diff_per_sell) as avg_diff_per_sell
        FROM
            {{ ref(table) }}
        WHERE
            {{ date_column }} >= CURRENT_DATE - INTERVAL '{{ i }} week'

        GROUP BY 
            asset, "G_WeekDay_N"

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    
    {% endfor %}

)

select * from weekly_sums
