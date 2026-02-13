select id, name, value + 10 as value from {{ source('src', 'test_table') }}
