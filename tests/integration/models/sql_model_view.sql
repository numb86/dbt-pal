{# Explicitly materialized as a view (which is also dbt's default for SQL models #}
{# with no config) to verify that view models work through delegation to dbt-bigquery. #}
{{ config(materialized='view') }}

select id, name, value + 10 as value from {{ source('src', 'test_table') }}
