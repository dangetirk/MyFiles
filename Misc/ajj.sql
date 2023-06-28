SELECT
    ReplayId , Id , IsDeleted , Name , CurrencyIsoCode , CreatedDate , CreatedById , LastModifiedDate , LastModifiedById , SystemModstamp , LastActivityDate , ConnectionReceivedId , ConnectionSentId , LLC_BI__Covenant2__c , LLC_BI__Account__c , LLC_BI__Contact__c , _ObjectType , _EventType 
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant;

-----COVENANT2
SELECT
 ReplayId , Id , OwnerId , IsDeleted , Name , CurrencyIsoCode , CreatedDate , CreatedById , LastModifiedDate , LastModifiedById , SystemModstamp , LastActivityDate , LastViewedDate , LastReferencedDate , ConnectionReceivedId , ConnectionSentId , LLC_BI__Active__c , LLC_BI__Compliance_Days_Prior__c , LLC_BI__Covenant_Type__c , LLC_BI__Days_Past_Next_Evaluation__c , LLC_BI__Days_Until_Next_Evaluation__c , LLC_BI__Detail__c , LLC_BI__Financial_Indicator_Value__c , LLC_BI__Frequency_Months__c , LLC_BI__Frequency__c , LLC_BI__Is_Template__c , LLC_BI__Last_Evaluation_Date__c , LLC_BI__Last_Evaluation_Status__c , LLC_BI__Next_Evaluation_Date__c , LLC_BI__Notes__c , LLC_BI__Required__c , LLC_BI__lookupKey__c , Relationship__c , LLC_BI__Effective_Date__c , LLC_BI__Linked_Spread_Statement_Record_Total__c , LLC_BI__Linked_Spread_Statement_Record__c , LLC_BI__Related_Covenant__c , Migration_ID__c , Integration_Source__c , LLC_BI__Comments__c , isTest__c , LLC_BI__Account__c , LLC_BI__Due_Date__c , CCS_Action__c , CCS_Bank_Entity__c , CCS_Covenant_Type__c , CCS_Description__c , CCS_Entity_Tested__c , CCS_Grace_Period_Days__c , CCS_IsFacilityBooked__c , CCS_Measure__c , CCS_Period_End__c , CCS_Restricted_User__c , CCS_Rolling_12M__c , CCS_State__c , CCS_Status__c , CCS_Test_Status__c , CCS_Tested_Against__c , CCS_Value__c , Covenant_Mgmt__c , CCS_Defination__c , _ObjectType , _EventType
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2;

----     rskcsp_ds_covenant_compliance2
SELECT
 ReplayId  , Id  , IsDeleted  , Name  , CurrencyIsoCode  , CreatedDate  , CreatedById  , LastModifiedDate  , LastModifiedById  , SystemModstamp  , LastActivityDate  , ConnectionReceivedId  , ConnectionSentId  , LLC_BI__Covenant__c  , LLC_BI__Approval_Date__c  , LLC_BI__Approved_By__c  , LLC_BI__Comments__c  , LLC_BI__Evaluated_By__c  , LLC_BI__Evaluation_Date__c  , LLC_BI__Exception_Date__c  , LLC_BI__Status__c  , LLC_BI__Historic_Financial_Indicator__c  , LLC_BI__Associated_Spread_Statement_Period__c  , LLC_BI__Associated_Statement_Period_Status__c  , LLC_BI__Automated_Testing_Status__c  , LLC_BI__Bundle__c  , LLC_BI__Effective_Date__c  , LLC_BI__Period_Key__c  , cm_Covenant_Compliance_Indicator_Value__c  , cm_Approver__c  , CCS_Action__c  , CCS_Denominator__c  , CCS_Frequency__c  , CCS_Numerator__c  , CCS_Outcome__c  , CCS_Period_End__c  , CCS_Result__c  , CCS_Value__c  , _ObjectType  , _EventType  
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2;

------rskcsp_ds_covenant_type
SELECT
    ReplayId   , Id   , OwnerId   , IsDeleted   , Name   , CurrencyIsoCode   , CreatedDate   , CreatedById   , LastModifiedDate   , LastModifiedById   , SystemModstamp   , LastActivityDate   , LastViewedDate   , LastReferencedDate   , ConnectionReceivedId   , ConnectionSentId   , LLC_BI__Category__c   , LLC_BI__lookupKey__c   , LLC_BI__Is_Financial_Indicator__c   , CCS_Description__c   , _ObjectType   , _EventType   
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type;

select
    ReplayId    , Id    , IsDeleted    , Name    , CurrencyIsoCode    , CreatedDate    , CreatedById    , LastModifiedDate    , LastModifiedById    , SystemModstamp    , LastActivityDate    , ConnectionReceivedId    , ConnectionSentId    , LLC_BI__Covenant2__c    , LLC_BI__Loan__c    , LLC_BI__Active__c    , CCS_UserProfile__c    , CCS_Restricted_User__c    , _ObjectType    , _EventType    
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant;


------------------STAGING SQL
SELECT
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant2__c, LLC_BI__Account__c, LLC_BI__Contact__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging;


-----COVENANT2
SELECT
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Active__c, LLC_BI__Compliance_Days_Prior__c, LLC_BI__Covenant_Type__c, LLC_BI__Days_Past_Next_Evaluation__c, LLC_BI__Days_Until_Next_Evaluation__c, LLC_BI__Detail__c, LLC_BI__Financial_Indicator_Value__c, LLC_BI__Frequency_Months__c, LLC_BI__Frequency__c, LLC_BI__Is_Template__c, LLC_BI__Last_Evaluation_Date__c, LLC_BI__Last_Evaluation_Status__c, LLC_BI__Next_Evaluation_Date__c, LLC_BI__Notes__c, LLC_BI__Required__c, LLC_BI__lookupKey__c, Relationship__c, LLC_BI__Effective_Date__c, LLC_BI__Linked_Spread_Statement_Record_Total__c, LLC_BI__Linked_Spread_Statement_Record__c, LLC_BI__Related_Covenant__c, Migration_ID__c, Integration_Source__c, LLC_BI__Comments__c, isTest__c, LLC_BI__Account__c, LLC_BI__Due_Date__c, CCS_Action__c, CCS_Bank_Entity__c, CCS_Covenant_Type__c, CCS_Description__c, CCS_Entity_Tested__c, CCS_Grace_Period_Days__c, CCS_IsFacilityBooked__c, CCS_Measure__c, CCS_Period_End__c, CCS_Restricted_User__c, CCS_Rolling_12M__c, CCS_State__c, CCS_Status__c, CCS_Test_Status__c, CCS_Tested_Against__c, CCS_Value__c, Covenant_Mgmt__c, CCS_Defination__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant2_staging;



----     rskcsp_ds_covenant_compliance2
SELECT
     --ReplayId
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant__c, LLC_BI__Approval_Date__c, LLC_BI__Approved_By__c, LLC_BI__Comments__c, LLC_BI__Evaluated_By__c, LLC_BI__Evaluation_Date__c, LLC_BI__Exception_Date__c, LLC_BI__Status__c, LLC_BI__Historic_Financial_Indicator__c, LLC_BI__Associated_Spread_Statement_Period__c, LLC_BI__Associated_Statement_Period_Status__c, LLC_BI__Automated_Testing_Status__c, LLC_BI__Bundle__c, LLC_BI__Effective_Date__c, LLC_BI__Period_Key__c, cm_Covenant_Compliance_Indicator_Value__c, cm_Approver__c, CCS_Action__c, CCS_Denominator__c, CCS_Frequency__c, CCS_Numerator__c, CCS_Outcome__c, CCS_Period_End__c, CCS_Result__c, CCS_Value__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_compliance2_staging;

------rskcsp_ds_covenant_type
SELECT
     -- ReplayId
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Category__c, LLC_BI__lookupKey__c, LLC_BI__Is_Financial_Indicator__c, CCS_Description__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging;


-----------------------$$$$$$$$$$$$$$$----CUREATED$$$$$$$$$$$$$$$$$$------
---rskcsp_ds_account_covenant_curated
SELECT
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant2__c, LLC_BI__Account__c, LLC_BI__Contact__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_account_covenant_curated;


-----COVENANT2
SELECT
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Active__c, LLC_BI__Compliance_Days_Prior__c, LLC_BI__Covenant_Type__c, LLC_BI__Days_Past_Next_Evaluation__c, LLC_BI__Days_Until_Next_Evaluation__c, LLC_BI__Detail__c, LLC_BI__Financial_Indicator_Value__c, LLC_BI__Frequency_Months__c, LLC_BI__Frequency__c, LLC_BI__Is_Template__c, LLC_BI__Last_Evaluation_Date__c, LLC_BI__Last_Evaluation_Status__c, LLC_BI__Next_Evaluation_Date__c, LLC_BI__Notes__c, LLC_BI__Required__c, LLC_BI__lookupKey__c, Relationship__c, LLC_BI__Effective_Date__c, LLC_BI__Linked_Spread_Statement_Record_Total__c, LLC_BI__Linked_Spread_Statement_Record__c, LLC_BI__Related_Covenant__c, Migration_ID__c, Integration_Source__c, LLC_BI__Comments__c, isTest__c, LLC_BI__Account__c, LLC_BI__Due_Date__c, CCS_Action__c, CCS_Bank_Entity__c, CCS_Covenant_Type__c, CCS_Description__c, CCS_Entity_Tested__c, CCS_Grace_Period_Days__c, CCS_IsFacilityBooked__c, CCS_Measure__c, CCS_Period_End__c, CCS_Restricted_User__c, CCS_Rolling_12M__c, CCS_State__c, CCS_Status__c, CCS_Test_Status__c, CCS_Tested_Against__c, CCS_Value__c, Covenant_Mgmt__c, CCS_Defination__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant2_curated;



----     rskcsp_ds_covenant_compliance2
SELECT
     --ReplayId    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant__c, LLC_BI__Approval_Date__c, LLC_BI__Approved_By__c, LLC_BI__Comments__c, LLC_BI__Evaluated_By__c, LLC_BI__Evaluation_Date__c, LLC_BI__Exception_Date__c, LLC_BI__Status__c, LLC_BI__Historic_Financial_Indicator__c, LLC_BI__Associated_Spread_Statement_Period__c, LLC_BI__Associated_Statement_Period_Status__c, LLC_BI__Automated_Testing_Status__c, LLC_BI__Bundle__c, LLC_BI__Effective_Date__c, LLC_BI__Period_Key__c, cm_Covenant_Compliance_Indicator_Value__c, cm_Approver__c, CCS_Action__c, CCS_Denominator__c, CCS_Frequency__c, CCS_Numerator__c, CCS_Outcome__c, CCS_Period_End__c, CCS_Result__c, CCS_Value__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_compliance2_curated;

------rskcsp_ds_covenant_type
SELECT
     -- ReplayId
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Category__c, LLC_BI__lookupKey__c, LLC_BI__Is_Financial_Indicator__c, CCS_Description__c

FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated;


----
