{{ config(
  materialized = 'table'
)}}


with 

cl as ( SELECT TO_DATE("Miladi", 'DD/MM/YYYY') as  date, *  from  {{source('utils', 'calendar')}}), 
xe as ( select * from {{source('raw', 'daily_bonbast')}}),

join_cl_xe as ( select * from xe left join cl  using (date))

select * from join_cl_xe

