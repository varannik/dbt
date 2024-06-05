-- macros/average_weekly.sql

{% macro average_weekly(asset, column, table) %}
    with base as (
        select 
            {{ column }} as value,
            date_trunc('week', {{ column }}::timestamp) as week_start
        from {{ table }}
        where asset = {{ asset }}
    ),
    weekly_avg as (
        select 
            week_start,
            avg(value) as weekly_average
        from base
        group by week_start
    )
    select * from weekly_avg
{% endmacro %}