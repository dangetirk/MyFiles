dbt test -x --profiles-dir ./profile --profile downstream_covenants_staging  --select covenant_test.sql

    dbt_expectations.expect_table_column_count_to_equal:

{% test %}
    SELECT COL1
    FROM (SELECT * FROM ECUR
          UNION ALL
          SELECT * from a) data
            group by col
having count(*)!=2)
)
{% pass_if:result == []%}