SELECT
ID,Name
,CONCAT(REPLACE(CAST(CreatedDate AS STRING ),' ','T'),'.000+0000') AS CreatedDate
,CONCAT(REPLACE(CAST(LastModifiedDate AS STRING ),' ','T'),'.000+0000') AS LastModifiedDate,CCS_Country_Code__c,
 CCS_Date_of_Birth__c,CCS_Email__c,CCS_Facility_ID__c,CCS_First_Name__c,CCS_Is_Card_Needed__c,CCS_Last_Name__c,CCS_Level_of_Control__c,CCS_Limit__c,CCS_Mobile_Number__c,CCS_Number_of_Cards__c,CCS_Title__c,CreatedById,CurrencyIsoCode,LastModifiedById FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_cardholder_curated
