-----------Raw
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type;
-----------Staging
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant2_staging;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_compliance2_staging;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_loan_covenant_staging;
-----------Curated
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant2_curated;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_compliance2_curated;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_account_covenant_curated;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_loan_covenant_curated;


truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging;
truncate table dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated;



SELECT ID,* FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
WHERE ID not IN
      ( 'a1e26000000uXZSAA2'
    ,'a1e26000000uXZTAA2'
    ,'a1e26000000uXZXAA2'
    ,'a1e26000000uXZcAAM'
    ,'a1e26000000uXZdAAM'
    ,'a1e26000000uXZeAAM'
    ,'a1e26000000uXZeAAM'
    ,'a1e26000000uXZeAAM'
    ,'a1e26000000uXZhAAM'
    ,'a1e26000000uXZmAAM'
    ,'a1e26000000uXZrAAM'
    ,'a1e26000000uXZwAAM'
    ,'a1e26000000uXZwAAM'
    ,'a1e26000000uXa1AAE'
    ,'a1e26000000uXa6AAE');

UPDATE dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
SET LastModifiedDate = NULL
WHERE ID ='a1e26000000uXakAAE';
UPDATE dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
SET ChangeEventHeader.changeType = NULL
WHERE ID ='a1e26000000uXapAAE';
UPDATE dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
SET ChangeEventHeader.changedFields = NULL
WHERE ID ='a1e26000000uXauAAE';
UPDATE dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
SET ChangeEventHeader.commitTimestamp = NULL
WHERE ID ='a1e26000000uXauAAE';
UPDATE dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
SET ChangeEventHeader.commitTimestamp = NULL
WHERE ID ='a1e26000000uXb9AAE';
UPDATE dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
SET _ObjectType = NULL
WHERE ID ='a1e26000000uXb9AAE';