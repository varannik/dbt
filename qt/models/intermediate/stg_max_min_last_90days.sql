{{ config(
  materialized = 'table'
)}}

{% set days_range = range(1, 91) %}

WITH

last_90_days as (select * from {{ ref('stg_last_90days_daily_rate_b')}}),

-- Calculate max and min for each date range and add range columns

rolling_stats AS (

    {% for i in days_range %}
        SELECT

            asset
            ,{{ i }} as range_period
            ,CURRENT_DATE - INTERVAL '{{ i }} days' as from_date
            ,MIN(sell) AS min_sell
            ,MAX(sell) AS max_sell
            ,MIN(buy)  AS min_buy
            ,MAX(buy)  AS max_buy
        FROM
            last_90_days
        WHERE
            date >= CURRENT_DATE - INTERVAL '{{ i }} days'
        GROUP BY
            asset

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)


select * from rolling_stats
