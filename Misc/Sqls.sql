Select Statements:
SELECT  * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant ;
SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type;
SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2 ;
SELECT *  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant ;
select  *from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2;


--Raw Table Count:
SELECT  "account_covenant"  as ObjectName, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant union all
SELECT "Covenant_type" CovenantType, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type union all
SELECT "covenant2" ObjectName, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2 union all
SELECT "loan_covenant" ObjectName, Count(*) cnt  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant union all
select  "covenant_compliance2" ObjectName, Count(*) cat from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2;
--Staging Table Count:
SELECT "account_covenant" as table_name, count(1) as count  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging union all
SELECT "covenant2" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant2_staging union all
SELECT "covenant_compliance2" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_compliance2_staging union all
SELECT "Covenant_type " as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging union all
SELECT "loan_covenant" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_loan_covenant_staging;

    --- Column Counts for All layers
    SELECT  table_name,count(*) column_count FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    where table_name in(
     "rskcsp_ds_account_covenant"
    ,"rskcsp_ds_covenant_type"
    ,"rskcsp_ds_covenant2"
    ,"rskcsp_ds_loan_covenant"
    ,"rskcsp_ds_covenant_compliance2"  )
    group by 1
    UNION ALL
    --Staging Layer
    SELECT  table_name,count(*) FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    where table_name in(  "rskcsp_ds_account_covenant_staging"
    ,"rskcsp_ds_covenant2_staging"
    ,"rskcsp_ds_covenant_compliance2_staging"
    ,"rskcsp_ds_covenant_type_staging"
    ,"rskcsp_ds_loan_covenant_staging")
    group by 1
    UNION ALL
    --Curated Layer

    SELECT  table_name,count(*) FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    where table_name in(  "rskcsp_ds_account_covenant_curated"
                          ,"rskcsp_ds_covenant2_curated"
                          ,"rskcsp_ds_covenant_type_curated"
                          ,"rskcsp_ds_loan_covenant_curated"
                          ,"rskcsp_ds_covenant_compliance2_curated")
    group by 1;











    dbt compile --profiles-dir ./profile --profile downstream_covenants_staging

    dbt test -x --profiles-dir ./profile --profile downstream_covenants_staging --select covenant_test.sql

--- All layer Covenant objects counts
with raw as (

                  SELECT  "account_covenant"  as table_name, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant union all
                  SELECT "Covenant_type" table_name, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type union all
                  SELECT "covenant2" table_name, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2 union all
                  SELECT "loan_covenant" table_name, Count(*) cnt  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant union all
                  select  "covenant_compliance2" table_name, Count(*) cat from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2),
       staging as (
                  SELECT "account_covenant" as table_name, count(1) as count  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging union all
                  SELECT "covenant2" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant2_staging union all
                  SELECT "covenant_compliance2" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_compliance2_staging union all
                  SELECT "Covenant_type" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging union all
                  SELECT "loan_covenant" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_loan_covenant_staging),
       Curated as (SELECT "account_covenant" as table_name, count(1) as count  FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_account_covenant_curated union all
                                       SELECT "covenant2" as table_name, count(1)  AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant2_curated union all
                                       SELECT "Covenant_type" as table_name, count(1) AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated union all
                                       SELECT "loan_covenant" as table_name, count(1)   AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_loan_covenant_curated union all
                                       SELECT "covenant_compliance2" as table_name, count(1) AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_compliance2_curated)
                  select a.table_name, b.cnt raw_layer_count, a.count staging_layer_count , c.count curated_layer_count from staging A, RAW B, curated c
                  WHERE A.TABLE_NAME = B.TABLE_NAME and a.table_name = c.table_name;




----
SELECT  'SELECT COUNT(1) COUNTT,'||"'"||TABLE_NAME||"'"||' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.'||table_name ||' UNION ALL' FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.INFORMATION_SCHEMA.TABLES
where table_name
in(
 'rskcsp_ds_ccs_deed_of_priority'
,'rskcsp_ds_ccs_security_case'
,'rskcsp_ds_collateral_ownership'
,'rskcsp_ds_collateral_valuation'
,'rskcsp_ds_loan_collateral_aggregate'
,'rskcsp_ds_collateral_plgd'
,'rskcsp_ds_collateral_mgmt'
,'rskcsp_ds_collateral_type'
,'rskcsp_ds_lien'
)




SELECT COUNT(1) COUNTT,'rskcsp_ds_collateral_type' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_collateral_type UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_lien' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_lien UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_collateral_mgmt' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_collateral_mgmt UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_ccs_security_case' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_ccs_security_case UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_collateral_ownership' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_collateral_ownership UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_ccs_deed_of_priority' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_ccs_deed_of_priority UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_collateral_valuation' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_collateral_valuation UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_loan_collateral_aggregate' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_collateral_aggregate UNION ALL
SELECT COUNT(1) COUNTT,'rskcsp_ds_collateral_plgd' AS TABLENAME  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_collateral_plgd;


    select id from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type;

     'a1j260000018SqqAAE'
    ,'a1j260000018SqvAAE'
    ,'a1j260000018Sr0AAE'
    ,'a1j260000018Sr1AAE'
    ,'a1j260000018Sr5AAE'
    ,'a1j260000018SrAAAU'
    ,'a1j260000018Sr6AAE'
    ,'a1j260000018SrFAAU'
    ,'a1j260000018SrKAAU'
    ,'a1j260000018SrPAAU'
    ,'a1j260000018SrQAAU'
    ,'a1j260000018SrUAAU'
    ,'a1j260000018SrZAAU'
    ,'a1j260000018SreAAE'
    ,'a1j260000018SreAAE'
    ,'a1j260000018SrQAAU'
    ,'a1j260000018SrQAAU'
    ,'a1j260000018SrAAAU'
    ,'a1j260000018Sr1AAE'


    select id from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2;

    'a1g26000003Pl6aAAC'
    ,'a1g26000003Pl6fAAC'
    ,'a1g26000003Pl6kAAC'
    ,'a1g26000003Pl6pAAC'
    ,'a1g26000003Pl6uAAC'
    ,'a1g26000003Pl6zAAC'
    ,'a1g26000003Pl74AAC'
    ,'a1g26000003Pl79AAC'
    ,'a1g26000003Pl7EAAS'
    ,'a1g26000003Pl7JAAS'
    ,'a1g26000003Pl7OAAS'
    ,'a1g26000003Pl7EAAS'
    ,'a1g26000003Pl6pAAC'
    ,'a1g26000003Pl7OAAS'
    ,'a1g26000003Pl7OAAS'
    select id from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant;
    select id from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant;
    id
     'a1e26000000uXIyAAM'
    ,'a1e26000000uXJ3AAM'
    ,'a1e26000000uXJ8AAM'
    ,'a1e26000000uXJDAA2'
    ,'a1e26000000uXJIAA2'
    ,'a1e26000000uXJNAA2'
    ,'a1e26000000uXJSAA2'
    ,'a1e26000000uXJXAA2'
    ,'a1e26000000uXJcAAM'
    ,'a1e26000000uXJhAAM'
    ,'a1e26000000uXJmAAM'
    ,'a1e26000000uXJDAA2'
    ,'a1e26000000uXJmAAM'
    ,'a1e26000000uXJmAAM'
    select id from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2 ;


     'a1h26000003CdRYAA0'
    ,'a1h26000003CdRdAAK'
    ,'a1h26000003CdReAAK'
    ,'a1h26000003CdRiAAK'
    ,'a1h26000003CdRnAAK'
    ,'a1h26000003CdRsAAK'
    ,'a1h26000003CdRxAAK'
    ,'a1h26000003CdS2AAK'
    ,'a1h26000003CdS7AAK'
    ,'a1h26000003CdSHAA0'
    ,'a1h26000003CdSMAA0'
    ,'a1h26000003CdSNAA0'
    ,'a1h26000003CdRjAAK'
    ,'a1h26000003CdSRAA0'
    ,'a1h26000003CdSWAA0'
    ,'a1h26000003CdReAAK'
    ,'a1h26000003CdS2AAK'
    ,'a1h26000003CdRsAAK'
    ,'a1h26000003CdRsAAK'


select * from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_account_covenant_curated
where id in ( 'a1e26000000uXIyAAM'
                 ,'a1e26000000uXJ3AAM'
                 ,'a1e26000000uXJ8AAM'
                 ,'a1e26000000uXJDAA2'
                 ,'a1e26000000uXJIAA2'
                 ,'a1e26000000uXJNAA2'
                 ,'a1e26000000uXJSAA2'
                 ,'a1e26000000uXJXAA2'
                 ,'a1e26000000uXJcAAM'
                 ,'a1e26000000uXJhAAM'
                 ,'a1e26000000uXJmAAM'
                 ,'a1e26000000uXJDAA2'
                 ,'a1e26000000uXJmAAM'
                 ,'a1e26000000uXJmAAM');
select * from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant2_curated
where id in ('    'a1g26000003Pl6aAAC'
                  ,'a1g26000003Pl6fAAC'
                  ,'a1g26000003Pl6kAAC'
                  ,'a1g26000003Pl6pAAC'
                  ,'a1g26000003Pl6uAAC'
                  ,'a1g26000003Pl6zAAC'
                  ,'a1g26000003Pl74AAC'
                  ,'a1g26000003Pl79AAC'
                  ,'a1g26000003Pl7EAAS'
                  ,'a1g26000003Pl7JAAS'
                  ,'a1g26000003Pl7OAAS'
                  ,'a1g26000003Pl7EAAS'
                  ,'a1g26000003Pl6pAAC'
                  ,'a1g26000003Pl7OAAS'
                  ,'a1g26000003Pl7OAAS'');
select * from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated
where id in (     'a1j260000018SqqAAE'
                 ,'a1j260000018SqvAAE'
                 ,'a1j260000018Sr0AAE'
                 ,'a1j260000018Sr1AAE'
                 ,'a1j260000018Sr5AAE'
                 ,'a1j260000018SrAAAU'
                 ,'a1j260000018Sr6AAE'
                 ,'a1j260000018SrFAAU'
                 ,'a1j260000018SrKAAU'
                 ,'a1j260000018SrPAAU'
                 ,'a1j260000018SrQAAU'
                 ,'a1j260000018SrUAAU'
                 ,'a1j260000018SrZAAU'
                 ,'a1j260000018SreAAE'
                 ,'a1j260000018SreAAE'
                 ,'a1j260000018SrQAAU'
                 ,'a1j260000018SrQAAU'
                 ,'a1j260000018SrAAAU'
                 ,'a1j260000018Sr1AAE'
);
select * from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_loan_covenant_curated
select * from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_compliance2_curated

where id in (     'a1h26000003CdRYAA0'
                 ,'a1h26000003CdRdAAK'
                 ,'a1h26000003CdReAAK'
                 ,'a1h26000003CdRiAAK'
                 ,'a1h26000003CdRnAAK'
                 ,'a1h26000003CdRsAAK'
                 ,'a1h26000003CdRxAAK'
                 ,'a1h26000003CdS2AAK'
                 ,'a1h26000003CdS7AAK'
                 ,'a1h26000003CdSHAA0'
                 ,'a1h26000003CdSMAA0'
                 ,'a1h26000003CdSNAA0'
                 ,'a1h26000003CdRjAAK'
                 ,'a1h26000003CdSRAA0'
                 ,'a1h26000003CdSWAA0'
                 ,'a1h26000003CdReAAK'
                 ,'a1h26000003CdS2AAK'
                 ,'a1h26000003CdRsAAK'
                 ,'a1h26000003CdRsAAK'
)

