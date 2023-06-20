SELECT
ID,Name,CONCAT(REPLACE(CAST(CreatedDate AS STRING ),' ','T'),'.000+0000') AS CreatedDate
,CONCAT(REPLACE(CAST(LastModifiedDate AS STRING ),' ','T'),'.000+0000') AS LastModifiedDate
 ,CCS_Amount__c,CCS_Current_Limit__c,CCS_Expiry_Date__c,CCS_Facility__c,CCS_Is_this_a_Temporary_Amendment__c,CCS_Proposed_Limit__c,CCS_Start_Date__c,CCS_Tenor__c,CCS_Utilisation__c,CreatedById,CurrencyIsoCode,LastModifiedById FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_limit_curated
