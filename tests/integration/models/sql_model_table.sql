{# Unlike sql_model (dbt's default: view), this model is materialized as a table, #}
{# which exercises the sql branch of pal's table materialization (db_materialization). #}
{{ config(materialized='table') }}

select id, name, value + 30 as value from {{ source('src', 'test_table') }}
