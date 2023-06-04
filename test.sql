SELECT TABLE_NAME,  count(1) cnt FROM (SELECT * FROM (

SELECT TABLE_NAME, column_name AS  COL , data_type ,1 seq  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS

where table_name in ('rskcsp_ds_spread_statement_period'
,'rskcsp_ds_classification'
,'rskcsp_ds_spread_record_total_classification'
,'rskcsp_ds_underwriting_bundle'
,'rskcsp_ds_spread_record_classification'
,'rskcsp_ds_debt_schedule'
,'rskcsp_ds_spread_statement_type'
,'rskcsp_ds_spread_statement_record_value'
,'rskcsp_ds_spread_projections_driver'
,'rskcsp_ds_spread_statement_record_total'
,'rskcsp_ds_spread_statement_record'
)

UNION ALL

SELECT TABLE_NAME, column_name COL, data_type ,2 seq FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS

where TABLE_NAME in ( 'rskcsp_ds_spread_statement_period_staging'
,'rskcsp_ds_classification_staging'
,'rskcsp_ds_spread_record_total_classification_staging'
,'rskcsp_ds_underwriting_bundle_staging'
,'rskcsp_ds_spread_record_classification_staging'
,'rskcsp_ds_debt_schedule_staging'
,'rskcsp_ds_spread_statement_type_staging'
,'rskcsp_ds_spread_statement_record_value_staging'
,'rskcsp_ds_spread_projections_driver_staging'
,'rskcsp_ds_spread_statement_record_total_staging'
,'rskcsp_ds_spread_statement_record_staging'
)

UNION ALL

SELECT TABLE_NAME, column_name COL , data_type , 3 seq FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS

where table_name in (

 'rskcsp_ds_spread_statement_period_curated'
,'rskcsp_ds_classification_curated'
,'rskcsp_ds_spread_record_total_classification_curated'
,'rskcsp_ds_underwriting_bundle_curated'
,'rskcsp_ds_spread_record_classification_curated'
,'rskcsp_ds_debt_schedule_curated'
,'rskcsp_ds_spread_statement_type_curated'
,'rskcsp_ds_spread_statement_record_value_curated'
,'rskcsp_ds_spread_projections_driver_curated'
,'rskcsp_ds_spread_statement_record_total_curated'
,'rskcsp_ds_spread_statement_record_curated'
))


GROUP BY  TABLE_NAME

ORDER BY 1
