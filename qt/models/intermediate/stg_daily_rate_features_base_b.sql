{{ config(
  materialized = 'table'
)}}


with 

drb_ as (select * from {{ ref('stg_daily_rate_b')}}),


base_features as (

  select 
  "date"
  ,asset
  ,sell
  ,buy
  ,"M_MidYear_T"
  ,"M_Quarter_N"
  ,"M_Month_N"
  ,EXTRACT(DOW FROM TO_DATE("Miladi", 'DD/MM/YYYY')) as "G_WeekDay_N"
  ,"M_Week_N"
  ,"J_Nime_T"
  ,"J_Quarter_N"
  ,"J_Month_N"
  ,"J_WeekDay_N"
  ,"J_Week_N"
  ,"J_Day_N"
  ,COALESCE(NULLIF("J_Holiday", 0), 0) AS "J_Holiday"
  ,"H_Month_N"
  ,"H_Day_N"

  -- Short-term moving averages (3, 5, 10 periods)
  -- Moving avarge past 3 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_short_3
  
  -- Moving avarge past 5 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_short_5

  -- Moving avarge past 10 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_short_10

  -- Medium-term moving averages (50, 100 periods)
  -- Moving avarge past 50 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_mid_50

  -- Moving avarge past 100 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_mid_100

  --Long-term moving averages (200, 300 periods) 
  -- Moving avarge past 200 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 200 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_long_200

  -- Moving avarge past 300 days 
  ,AVG(sell) OVER (
        PARTITION BY asset 
        ORDER BY date 
        ROWS BETWEEN 300 PRECEDING AND 1 PRECEDING
    ) AS moving_avg_long_300

    -- Create lag features for 1 day, 7 days, and 30 days
  ,LAG(sell, 1) OVER (PARTITION BY asset ORDER BY date) AS lag_1_day
  ,LAG(sell, 7) OVER (PARTITION BY asset ORDER BY date) AS lag_7_days
  ,LAG(sell, 30) OVER (PARTITION BY asset ORDER BY date) AS lag_30_days

  -- Rolling 3-day standard deviation (volatility)
  ,STDDEV_SAMP(sell) OVER (
        PARTITION BY asset
        ORDER BY date
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS rolling_stddev_3_days
    
  -- Rolling 5-day standard deviation (volatility)
  ,STDDEV_SAMP(sell) OVER (
        PARTITION BY asset
        ORDER BY date
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS rolling_stddev_5_days

  -- Rolling 10-day standard deviation (volatility)
  ,STDDEV_SAMP(sell) OVER (
        PARTITION BY asset
        ORDER BY date
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rolling_stddev_10_days
  
  -- Calculate 1-day percentage change (Rate of Change)
  ,((LAG(sell, 1) OVER (PARTITION BY asset ORDER BY date) - LAG(sell, 2) OVER (PARTITION BY asset ORDER BY date)) / LAG(sell, 2) OVER (PARTITION BY asset ORDER BY date)) * 100 AS roc_1_day
  ,((LAG(sell, 1) OVER (PARTITION BY asset ORDER BY date) - LAG(sell, 7) OVER (PARTITION BY asset ORDER BY date)) / LAG(sell, 7) OVER (PARTITION BY asset ORDER BY date)) * 100 AS roc_7_day

  from drb_

),

gain_loss AS (
    SELECT
        *
        -- Separate gains and losses
        ,CASE WHEN roc_1_day > 0 THEN roc_1_day ELSE 0 END AS gain
        ,CASE WHEN roc_1_day < 0 THEN ABS(roc_1_day) ELSE 0 END AS loss

    FROM base_features
),

rolling_avg_RS AS (
    SELECT
      * 
        -- Calculate 14-day rolling average of gains and losses
      ,AVG(gain) OVER (PARTITION BY asset ORDER BY date ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING) AS avg_gain_13days
      ,AVG(loss) OVER (PARTITION BY asset ORDER BY date ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING) AS avg_loss_13days
    FROM gain_loss
),

RSI AS (
  SELECT
    *
    -- Calculate Relative Strength (RS)
    ,CASE
        WHEN avg_loss_13days = 0 THEN 100  -- To handle division by zero, when avg_loss is zero, RSI is 100
        ELSE avg_gain_13days / avg_loss_13days
    END AS relative_strength
    -- Calculate RSI
    ,CASE
        WHEN avg_loss_13days = 0 THEN 100  -- When avg_loss is zero, RSI is 100
        ELSE 100 - (100 / (1 + (avg_gain_13days / avg_loss_13days)))
    END AS rsi_14_days
FROM rolling_avg_RS
),



Bollinger_Bands as (
-- Bollinger Bands Formula:
-- Middle Band (MA): The simple moving average of the sell column.
-- Upper Band: Moving average + (2 × standard deviation).
-- Lower Band: Moving average - (2 × standard deviation).

select 
    *
    ,moving_avg_short_10 AS middle_band
    -- Calculate the Upper Band: Moving Average + 2 * Standard Deviation
    ,moving_avg_short_10 + (2 * rolling_stddev_10_days) AS upper_band
    -- Calculate the Lower Band: Moving Average - 2 * Standard Deviation
    ,moving_avg_short_10 - (2 * rolling_stddev_10_days) AS lower_band

    from RSI

) 

select * from Bollinger_Bands



