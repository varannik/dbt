{{ config(
  materialized = 'table'
)}}


with 

-- cl as ( SELECT TO_DATE("Miladi", 'DD/MM/YYYY') as  date, *  from  {{source('utils', 'calendar')}}), 
xe_past as ( select * from {{source('raw', 'daily_bonbast')}}),
xe_now as (select * from {{ ref('stg_last_price_b') }}),


xe_full as ( 

    select * from xe_past
    union all 
    select * from xe_now

)

select 
    
    date::date
    ,asset
    ,sell
    ,buy

from xe_full