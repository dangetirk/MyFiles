{% macro expression_is_true(model, expression, fail_calc, error_if, columns) %}

{% set table_1 = model.alias('table_1') %}
{% set table_2 = model.alias('table_2') %}

with

table_1_cte as (
  select
    'expected' as source,
    {{ columns|join(', ') }}
  from {{ table_1 }}
),

table_2_cte as (
  select
    'actual' as source,
    case when z > 1 then 1 else 2 end as a,
    {{ columns|join(', ') }}
  from {{ table_2 }}
),

combined as (
  select * from table_1_cte
  union all
  select * from table_2_cte
),

aggregated as (
  select
    max(source) as source,
    {{ columns|join(', ') }},
    count(*) as cnt
  from combined
  group by {{ columns|join(', ') }}
  having count(*) = 1
),

row_counts as (
  select
    count(*) as table_1_rows,
    sum(case when source = 'expected' then 1 else 0 end) as table_2_rows,
    count(*) as differences,
    sum(case when cnt = 2 then 1 else 0 end) as matching_rows
  from aggregated
)

select
  {{ fail_calc }} as failure_reason,
  count(*) as failures
from row_counts
where not ({{ expression }}) or ({{ error_if }})
group by {{ fail_calc }}

{% endmacro %}

And update the schema.yml file accordingly:
version: 2

models:
  - name: table_1
    database: db01
    schema: ds01
    tests:
      - dbt_utils.expression_is_true:
          expression: "table_1_rows > 0 AND table_2_rows > 0 AND differences = 0"
          fail_calc: "differences"
          error_if: "table_1_rows = 0 OR table_2_rows = 0"
          columns: ["a", "b", "c", "d"]

  - name: table_2
    database: db01
    schema: ds02
