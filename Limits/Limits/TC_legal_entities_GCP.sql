SELECT
 Id,Name,CreatedById
,CONCAT(REPLACE(CAST(CreatedDate AS STRING ),' ','T'),'.000+0000') AS CreatedDate
,CONCAT(REPLACE(CAST(LastModifiedDate AS STRING ),' ','T'),'.000+0000') AS LastModifiedDate,
CurrencyIsoCode,LastModifiedById,LLC_BI__Account__c,LLC_BI__Borrower_Type__c,LLC_BI__Contingent_Amount__c,LLC_BI__Contingent_Type__c,LLC_BI__Loan__c,LLC_BI__Ownership__c,LLC_BI__Product_Package__c,Migration_ID__c,RecordTypeId FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_legal_entities_curated
