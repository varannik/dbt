{{ config(
  materialized = 'table'
)}}

with

bollinger_bands_columns as (
  select 
    {% set columns = adapter.get_columns_in_relation(ref('stg_daily_rate_features_base_b')) %}
  {% set selected_columns = [] %}

  {% for column in columns %}
    {% if 'band' in column.name %}
      {% do selected_columns.append(column.name) %}
    {% endif %}
  {% endfor %}

  {{ selected_columns | join(", ") }}
    ,sell
    ,date
    ,asset


  FROM {{ref('stg_daily_rate_features_base_b')}}

),

bb_sort as (
    SELECT *,
         ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS rank
    FROM bollinger_bands_columns
),

bb_10days as (
    SELECT *
    FROM bb_sort
    WHERE rank <= 10
    ORDER BY asset , date 
)

select * from bb_10days
