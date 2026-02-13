{% materialization table, adapter='pal', supported_languages=['python', 'sql'] -%}

{%- set language = model['language'] -%}

{%- if language == 'python' -%}

  {# "When written as follows, the Adapter's submit_python_job() method is called" #}
  {# "dbt-core is designed that way" #}
  {% call statement('main', language='python') -%}
    {{ compiled_code }}
  {%- endcall %}

  {{ return({'relations': []}) }}

{%- elif language == 'sql' -%}

  {# "Call PalAdapterWrapper's db_materialization()" #}
  {{ return(adapter.db_materialization(context, "table")) }}

{%- endif -%}

{% endmaterialization %}
