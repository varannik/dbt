{{ config(
  materialized = 'table'
)}}


with 

cl as ( SELECT TO_DATE("Miladi", 'DD/MM/YYYY') as  date, *  from  {{source('utils', 'calendar')}}), 
xe_past as ( select * from {{source('raw', 'daily_bonbast')}}),
xe_now as (select * from {{ ref('stg_last_price_b') }}),


xe_full as ( 

  select * from xe_past
  union  
  select * from xe_now

),
xe_full_refine as (

select 
    
    date::date
    ,asset
    ,sell
    ,buy

from xe_full
),


join_cl_xe as ( select * from xe_full_refine left join cl  using (date))

select * from join_cl_xe

