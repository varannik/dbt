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
  select 
    (("date" AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Tehran')::DATE AS date
    ,asset
    ,sell
    ,buy
  
  from xe_now

),
xe_full_refine as (

select 
    
    date::date
    ,asset
    ,sell
    ,buy

from xe_full
where asset NOT IN (
'½ Azadi'
,'¼ Azadi'
,'Azadi'
,'Emami'
,'Gerami'
,'Turkmenistani Manat'
,'Georgian Lari'
,'Kyrgystani Som'
,'Tajikistani Somoni'
,'South Korean won'
,'New Zealand Dollar'
,'Pakistani Rupee'
,'Syrian Pound'
 )
),


join_cl_xe as ( select * from xe_full_refine left join cl  using (date))

select * from join_cl_xe


