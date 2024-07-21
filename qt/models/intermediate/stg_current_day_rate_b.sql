{{ config(
  materialized = 'table'
)}}

with

xe as ( select * from {{source('raw', 'realtime_bonbast')}}),

xe_filtered as (
    select *
    from xe
    where 
        date_trunc('day', date) = current_date
        and 
        (
            extract(hour from date) > 7
            or (extract(hour from date) = 7 and extract(minute from date) >= 29)
        )
        and
        (
            extract(hour from date) < 15
            or (extract(hour from date) = 15 and extract(minute from date) <= 32)
        )
)


select *
from xe_filtered
