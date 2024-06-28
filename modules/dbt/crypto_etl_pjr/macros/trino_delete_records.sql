{% macro trino_delete_records(date_column_name, delete_days) %}

  {% set query %}
    delete from {{ this }} where {{ date_column_name }} >= date_add('day', {{ delete_days }}, current_date)
  {% endset %}

  {% do run_query(query) %}
{% endmacro %}

