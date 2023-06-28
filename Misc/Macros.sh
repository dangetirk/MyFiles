{% macro bigquery_test_string(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as string) is null
{% endmacro %}

{% macro bigquery_test_int64(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as int64) is null
{% endmacro %}

{% macro bigquery_test_float64(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as float64) is null
{% endmacro %}

{% macro bigquery_test_bool(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as bool) is null
{% endmacro %}

{% macro bigquery_test_date(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as date) is null
{% endmacro %}

{% macro bigquery_test_timestamp(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as timestamp) is null
{% endmacro %}

{% macro bigquery_test_time(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as time) is null
{% endmacro %}

{% macro bigquery_test_datetime(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as datetime) is null
{% endmacro %}

{% macro bigquery_test_bytes(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as bytes) is null
{% endmacro %}

{% macro bigquery_test_numeric(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as numeric) is null
{% endmacro %}

{% macro bigquery_test_bignumeric(model, column_name) %}
  select {{ column_name }}
  from {{ model }}
  where safe_cast({{ column_name }} as bignumeric) is null
{% endmacro %}
