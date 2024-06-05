{{ config(materialized='table') }}


{% set table = 'stg_diff_daily_with_avg_weekly_b' %}
{% set date_column = 'date' %}
{% set weeks_range = range(1, 50) %}

with weekly_sums as (

    {% for i in weeks_range %}

        SELECT 
            asset
            ,week_day_pr
            ,{{ i }} as week_number
            ,SUM(diff_buy) as sum_diff_buy 
            ,SUM(diff_sell) as sum_diff_sell
        FROM
            {{ ref(table) }}
        WHERE
            {{ date_column }} >= CURRENT_DATE - INTERVAL '{{ i }} week'

        GROUP BY 
            asset, week_day_pr

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    
    {% endfor %}

)

select * from weekly_sums
