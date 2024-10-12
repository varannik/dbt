{{ config(
  materialized = 'table'
)}}

with

ma_columns as (
  select 
  

  {% set columns = adapter.get_columns_in_relation(ref('stg_daily_rate_features_base_b')) %}
  {% set selected_columns = [] %}

  {% for column in columns %}
    {% if 'moa' in column.name %}
      {% do selected_columns.append(column.name) %}
    {% endif %}
  {% endfor %}

  {{ selected_columns | join(", ") }}
  ,sell
  ,date
  ,asset

  FROM {{ref('stg_daily_rate_features_base_b')}}

),

ma_sort as (
    SELECT *,
         ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS rank
    FROM ma_columns
),

ma_10days as (
    SELECT *
    FROM ma_sort
    WHERE rank <= 10
    ORDER BY asset , date 
)

select * from ma_10days

