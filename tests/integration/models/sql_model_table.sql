{# Unlike sql_model_view (dbt's default: view), this model is materialized as a table. #}
{# Together they verify that SQL models are delegated to dbt-bigquery's materializations. #}
{{ config(materialized='table') }}

select id, name, value + 30 as value from {{ source('src', 'test_table') }}
