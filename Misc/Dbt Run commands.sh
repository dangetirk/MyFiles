dbt docs serve --profiles-dir ./profile --profile downstream_covenants_staging 
dbt docs generate --profiles-dir ./profile --profile downstream_covenants_staging 
gcloud auth application-default login

--To Run staging models
dbt run  --profiles-dir ./profile --profile downstream_covenants_staging
----To test the models
dbt test  --profiles-dir ./profile --profile downstream_covenants_staging


dbt run -x --profiles-dir ./profile --profile downstream_covenants_staging --models staging.*


dbt test -x --profiles-dir ./profile —downstream_covenants_raw --select covenant_test.sql


dbt run --profiles-dir ./profile --profile downstream_covenants_staging --select elementary
dbt test -x --profiles-dir ./profile --profile downstream_covenants_staging --select covenant_test.sql


dbt debug --profiles-dir ./profile --profile downstream_covenants_staging
dbt compile --profiles-dir ./profile --profile downstream_covenants_staging

dbt test -x --profiles-dir ./profile —dbttest

dbt test -x --profiles-dir ./profile --profile dbttest

-- To Run particualar Model
dbt run -x --profiles-dir ./profile --profile downstream_covenants_staging --select rskcsp_ds_loan_covenant_staging

-- To Test particualar Model

dbt test -x --profiles-dir ./profile --profile downstream_covenants_staging --select rskcsp_ds_account_covenant_staging

rskcsp_ds_account_covenant_staging

dbt run-operation --profiles-dir ./profile --profile downstream_covenants_staging elementary.generate_elementary_cli_profile


dbt test  --profiles-dir ./profile --profile downstream_covenants_raw --select source:covenants_raw.rskcsp_ds_covenant_type

dbt test  --profiles-dir ./profile --profile downstream_covenants_raw --select source:covenants_raw.rskcsp_ds_covenant_type

dbt seed   --profiles-dir ./profile --profile downstream_covenants_raw --select ncino_Account
dbt seed   --profiles-dir ./profile --profile downstream_covenants_raw --select ncino_Account
dbt seed   --profiles-dir ./profile --profile downstream_covenants_raw --select ncino_Account

dbt run -x --profiles-dir ./profile --profile downstream_covenants_staging --select rskcsp_ds_account_covenant_staging
dbt run -x --profiles-dir ./profile --profile downstream_covenants_curated --select rskcsp_ds_account_covenant_curated

##########################test raw
dbt test -x --profiles-dir ./profile --profile downstream_covenants_raw --select rskcsp_ds_account_covenant_staging

dbt test -x --profiles-dir ./profile --profile downstream_covenants_staging --select rskcsp_ds_account_covenant_staging

dbt run --profiles-dir ./profile --profile collateral_staging --select rskcsp_ds_ccs_deed_of_priority_staging