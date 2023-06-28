Remove blank line in intelj
    ^\s*^


SELECT  table_name,count(*) count FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(
 "rskcsp_ds_account_covenant"
,"rskcsp_ds_covenant_type"
,"rskcsp_ds_covenant2"
,"rskcsp_ds_loan_covenant"
,"rskcsp_ds_covenant_compliance2")
group by 1
union all
SELECT  table_name,count(*) count FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(
 "rskcsp_ds_account_covenant_staging"
,"rskcsp_ds_covenant2_staging"
,"rskcsp_ds_covenant_compliance2_staging"
,"rskcsp_ds_covenant_type_staging"
,"rskcsp_ds_loan_covenant_staging")
group by 1
union all
SELECT  table_name,count(*) count FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(  "rskcsp_ds_account_covenant_curated"
                      ,"rskcsp_ds_covenant2_curated"
                      ,"rskcsp_ds_covenant_type_curated"
                      ,"rskcsp_ds_loan_covenant_curated"
                      ,"rskcsp_ds_covenant_compliance2_curated")
group by 1
union all
SELECT  table_name,count(*) count FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(  "covenant_type"
                      ,"account_covenant"
                      ,"covenant2"
                      ,"loan_covenant"
                      ,"covenant_compliance2")
group by 1
order by 1;


----Colletral Raw
SELECT  cast('select  "' ||  table_name|| '" as tablename,  count(1) as count from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.'||table_name|| ' union all'  as STRING) as query

FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.INFORMATION_SCHEMA.TABLES
where table_name  in
    (

    'rskcsp_ds_ccs_deed_of_priority'
    ,'rskcsp_ds_ccs_security_case'
    ,'rskcsp_ds_collateral_ownership'
    ,'rskcsp_ds_collateral_type'
    ,'rskcsp_ds_collateral_valuation'
    ,'rskcsp_ds_collateral_mgmt'
    ,'rskcsp_ds_lien'
    ,'rskcsp_ds_collateral_plgd'
    ,'rskcsp_ds_loan_collateral_aggregate'
    );
----Colletral Staging
SELECT  cast('select  "' ||  table_name|| '" as tablename,  count(1) as count from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.'||table_name|| ' union all'  as STRING) as query

FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.INFORMATION_SCHEMA.TABLES
where table_name  in
      (

       'rskcsp_ds_ccs_deed_of_priority_staging'
          ,'rskcsp_ds_ccs_security_case_staging'
          ,'rskcsp_ds_collateral_ownership_staging'
          ,'rskcsp_ds_collateral_type_staging'
          ,'rskcsp_ds_collateral_valuation_staging'
          ,'rskcsp_ds_collateral_mgmt_staging'
          ,'rskcsp_ds_lien_staging'
          ,'rskcsp_ds_collateral_plgd_staging'
          ,'rskcsp_ds_loan_collateral_aggregate_staging'
          );

----Colletral curated
SELECT  cast('select  "' ||  table_name|| '" as tablename,  count(1) as count from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.'||table_name|| ' union all'  as STRING) as query

FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.INFORMATION_SCHEMA.TABLES
where table_name  in
      (

       'rskcsp_ds_ccs_deed_of_priority_curated'
          ,'rskcsp_ds_ccs_security_case_curated'
          ,'rskcsp_ds_collateral_ownership_curated'
          ,'rskcsp_ds_collateral_type_curated'
          ,'rskcsp_ds_collateral_valuation_curated'
          ,'rskcsp_ds_collateral_mgmt_curated'
          ,'rskcsp_ds_lien_curated'
          ,'rskcsp_ds_collateral_plgd_curated'
          ,'rskcsp_ds_loan_collateral_aggregate_curated'
          );