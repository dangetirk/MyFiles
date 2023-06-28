-----Column counts for all layers - Raw - Staging - Curated


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
order by 1;


 CONSUMPTION as (SELECT "account_covenant" as table_name, count(1) as count  FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.account_covenant union all
                                       SELECT "covenant2" as table_name, count(1)  AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.covenant2  union all
                                       SELECT "Covenant_type" as table_name, count(1) AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.covenant_type union all
                                       SELECT "loan_covenant" as table_name, count(1)   AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.loan_covenant union all
                                       SELECT "covenant_compliance2" as table_name, count(1) AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.covenant_compliance2 )

CONSUMPTION AS (
SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.covenant_type ORDER BY ID;
SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.account_covenant ORDER BY ID;
SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.covenant2 ORDER BY ID;
SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_consumption.covenant_compliance2 ORDER BY ID;

With raw as (
SELECT   row_number() over(order by field_path) rno,'Account_Covenant' table_name,   field_path ,data_type FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(
 "rskcsp_ds_account_covenant"
 ) and column_name <>'ChangeEventHeader' and column_name<>'ReplayId'






 ),
staging as (
SELECT  row_number() over(order by field_path) rno,  'Account_Covenant' table_name, field_path ,data_type  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(
 "rskcsp_ds_account_covenant_staging")
 ),

 curated as (
SELECT  row_number() over(order by field_path) rno,  'Account_Covenant' table_name, field_path,data_type  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name in(
 "rskcsp_ds_account_covenant_curated")
 )

 select   a.table_name,a.field_path raw_layer,a.data_type,b.field_path Staging_layer,b.data_type, c.field_path Curated_layer,c.data_type from raw a     full join staging b
 on ( a.rno = b.rno  )
 full join   curated c
 on ( a.rno=c.rno )
 order by 1 nulls last, 2 nulls last,3





