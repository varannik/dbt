{{ config(
  materialized = 'table'
)}}

with

full_calendar as ( select * from  {{source('utils', 'calendar')}}),

full_cl as (

SELECT 
	TO_DATE("Miladi", 'DD/MM/YYYY') as G_Date_Ref
  ,"M_Week_N" AS G_Week_N
  ,"M_Month_N" as G_Month_N
  ,EXTRACT(year FROM TO_DATE("Miladi", 'DD/MM/YYYY')) AS G_Year_N
  ,"M_Quarter_N" AS G_Quarter_N
	,"J_WeekDay_N" as J_WeekDay_N
	,"J_Week_N" as J_Week_N
	,"J_Month_N" as J_Month_N
	,"J_Year_N" as J_Year_N
	,"J_Quarter_N" as J_Quarter_N
	
from full_calendar
),

cur_full_cl as (
  SELECT 
    G_Date_Ref,
    G_Week_N,
    G_Year_N,
    G_Month_N,
    G_Quarter_N,
    J_Year_N,
    J_WeekDay_N,
    J_Week_N,
    J_Month_N,
    J_Quarter_N
    
  from full_cl
  where  G_Date_Ref = CURRENT_DATE 
),

cur_week_J as (
  select full_cl.G_Date_Ref
  ,'cur_week_J' as range_data

  from cur_full_cl 
  inner join full_cl
  using (J_Year_N,J_Week_N)
),

cur_week_G as (
  select full_cl.G_Date_Ref
  ,'cur_week_G' as range_data
  from cur_full_cl 
  inner join full_cl
  using (G_Year_N,G_Week_N)
),

cur_month_J as (
  select full_cl.G_Date_Ref
  ,'cur_month_J' as range_data

  from cur_full_cl 
  inner join full_cl
  using (J_Year_N,J_Month_N)

),

cur_month_G as (
  select full_cl.G_Date_Ref
  ,'cur_month_G' as range_data

  from cur_full_cl 
  inner join full_cl
  using (G_Year_N,G_Month_N)

),

cur_quarter_G as (
  select full_cl.G_Date_Ref
  ,'cur_quarter_G' as range_data
  from cur_full_cl 
  inner join full_cl
  using (G_Year_N,G_Quarter_N)

),

cur_quarter_J as (
  select full_cl.G_Date_Ref
  ,'cur_quarter_J' as range_data
  from cur_full_cl 
  inner join full_cl
  using (J_Year_N,J_Quarter_N)

) ,

union_ranges as (
  select * from cur_week_G
  UNION ALL 
  select * from cur_week_J
  UNION ALL 
  select * from cur_month_J
  UNION ALL 
  select * from cur_month_G
  UNION ALL 
  select * from cur_quarter_J
  UNION ALL
  select * from cur_quarter_G

)

select * from union_ranges



