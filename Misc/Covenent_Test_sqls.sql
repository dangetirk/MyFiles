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
------------------$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$---STAGING SQL--$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
----account_covenant
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
    --ReplayId
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant__c, LLC_BI__Approval_Date__c, LLC_BI__Approved_By__c, LLC_BI__Comments__c, LLC_BI__Evaluated_By__c, LLC_BI__Evaluation_Date__c, LLC_BI__Exception_Date__c, LLC_BI__Status__c, LLC_BI__Historic_Financial_Indicator__c, LLC_BI__Associated_Spread_Statement_Period__c, LLC_BI__Associated_Statement_Period_Status__c, LLC_BI__Automated_Testing_Status__c, LLC_BI__Bundle__c, LLC_BI__Effective_Date__c, LLC_BI__Period_Key__c, cm_Covenant_Compliance_Indicator_Value__c, cm_Approver__c, CCS_Action__c, CCS_Denominator__c, CCS_Frequency__c, CCS_Numerator__c, CCS_Outcome__c, CCS_Period_End__c, CCS_Result__c, CCS_Value__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_compliance2_curated;

------rskcsp_ds_covenant_type
SELECT
    -- ReplayId
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Category__c, LLC_BI__lookupKey__c, LLC_BI__Is_Financial_Indicator__c, CCS_Description__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated;
--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

--- All layer Covenant objects counts
with
        raw as (
SELECT  "account_covenant"  as table_name, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant union all
SELECT "Covenant_type" table_name, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type union all
SELECT "covenant2" table_name, Count(*) cnt FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2 union all
SELECT "loan_covenant" table_name, Count(*) cnt  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_loan_covenant union all
select  "covenant_compliance2" table_name, Count(*) cat from dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2),
       staging as (
SELECT "account_covenant" as table_name, count(1) as count  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging union all
SELECT "covenant2" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant2_staging union all
SELECT "covenant_compliance2" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_compliance2_staging union all
SELECT "Covenant_type " as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging union all
SELECT "loan_covenant" as table_name, count(1)  FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_loan_covenant_staging),
       Curated as (
SELECT "account_covenant" as table_name, count(1) as count  FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_account_covenant_curated union all
SELECT "covenant2" as table_name, count(1)  AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant2_curated union all
SELECT "Covenant_type " as table_name, count(1) AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_type_curated union all
SELECT "loan_covenant" as table_name, count(1)   AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_loan_covenant_curated union all
SELECT "covenant_compliance2" as table_name, count(1) AS COUNT FROM  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_covenant_compliance2_curated)
select a.table_name, b.cnt raw_layer_count, a.count staging_layer_count , c.count curated_layer_count from staging A, RAW B, curated c
WHERE A.TABLE_NAME = B.TABLE_NAME and a.table_name = c.table_name;

--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@---RAW VS STAGING

--ACCOUNT COVENANT
SELECT "RAW" AS LAYER,

       Id , IsDeleted , Name , CurrencyIsoCode ,SAFE_CAST(TIMESTAMP_MILLIS(CreatedDate) AS DATETIME) CreatedDate , CreatedById ,SAFE_CAST(TIMESTAMP_MILLIS(LastModifiedDate) AS DATETIME) LastModifiedDate , LastModifiedById , SAFE_CAST(TIMESTAMP_MILLIS(SystemModstamp) AS DATETIME) SystemModstamp , SAFE_CAST(TIMESTAMP_MILLIS(LastActivityDate) AS DATETIME) LastActivityDate , ConnectionReceivedId , ConnectionSentId , LLC_BI__Covenant2__c , LLC_BI__Account__c , LLC_BI__Contact__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
UNION ALL
SELECT "STAGING" AS LAYER,
       Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant2__c, LLC_BI__Account__c, LLC_BI__Contact__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging
order by id,name;

--COVENANT 2S
SELECT
    Id , OwnerId , IsDeleted , Name , CurrencyIsoCode , SAFE_CAST(TIMESTAMP_MILLIS(CreatedDate) AS DATETIME) as CreatedDate , CreatedById , SAFE_CAST(TIMESTAMP_MILLIS(LastModifiedDate) AS DATETIME) as LastModifiedDate  ,LastModifiedById, SAFE_CAST(TIMESTAMP_MILLIS(SystemModstamp) AS DATETIME)as  SystemModstamp , SAFE_CAST(TIMESTAMP_MILLIS(LastActivityDate) AS DATE) as LastActivityDate , SAFE_CAST(TIMESTAMP_MILLIS(LastViewedDate) AS DATETIME)   as LastViewedDate ,SAFE_CAST(TIMESTAMP_MILLIS(LastReferencedDate) AS DATETIME)  as LastReferencedDate , ConnectionReceivedId , ConnectionSentId , LLC_BI__Active__c , LLC_BI__Compliance_Days_Prior__c , LLC_BI__Covenant_Type__c , LLC_BI__Days_Past_Next_Evaluation__c , LLC_BI__Days_Until_Next_Evaluation__c , LLC_BI__Detail__c , LLC_BI__Financial_Indicator_Value__c , LLC_BI__Frequency_Months__c , LLC_BI__Frequency__c , LLC_BI__Is_Template__c , SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Last_Evaluation_Date__c) AS DATE) as LLC_BI__Last_Evaluation_Date__c , LLC_BI__Last_Evaluation_Status__c , SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Next_Evaluation_Date__c) AS DATE) as LLC_BI__Next_Evaluation_Date__c , LLC_BI__Notes__c , LLC_BI__Required__c , LLC_BI__lookupKey__c , Relationship__c , SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Effective_Date__c) AS DATE) as  LLC_BI__Effective_Date__c , LLC_BI__Linked_Spread_Statement_Record_Total__c , LLC_BI__Linked_Spread_Statement_Record__c , LLC_BI__Related_Covenant__c , Migration_ID__c , Integration_Source__c , LLC_BI__Comments__c , isTest__c , LLC_BI__Account__c ,  SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Due_Date__c) AS DATE) as LLC_BI__Due_Date__c , CCS_Action__c , CCS_Bank_Entity__c , CCS_Covenant_Type__c , CCS_Description__c , CCS_Entity_Tested__c , CCS_Grace_Period_Days__c , CCS_IsFacilityBooked__c , CCS_Measure__c ,  SAFE_CAST(TIMESTAMP_MILLIS(CCS_Period_End__c) AS DATE) as CCS_Period_End__c , CCS_Restricted_User__c , CCS_Rolling_12M__c , CCS_State__c , CCS_Status__c , CCS_Test_Status__c , CCS_Tested_Against__c , CCS_Value__c , Covenant_Mgmt__c , CCS_Defination__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant2
EXCEPT DISTINCT
SELECT
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Active__c, LLC_BI__Compliance_Days_Prior__c, LLC_BI__Covenant_Type__c, LLC_BI__Days_Past_Next_Evaluation__c, LLC_BI__Days_Until_Next_Evaluation__c, LLC_BI__Detail__c, LLC_BI__Financial_Indicator_Value__c, LLC_BI__Frequency_Months__c, LLC_BI__Frequency__c, LLC_BI__Is_Template__c, LLC_BI__Last_Evaluation_Date__c, LLC_BI__Last_Evaluation_Status__c, LLC_BI__Next_Evaluation_Date__c, LLC_BI__Notes__c, LLC_BI__Required__c, LLC_BI__lookupKey__c, Relationship__c, LLC_BI__Effective_Date__c, LLC_BI__Linked_Spread_Statement_Record_Total__c, LLC_BI__Linked_Spread_Statement_Record__c, LLC_BI__Related_Covenant__c, Migration_ID__c, Integration_Source__c, LLC_BI__Comments__c, isTest__c, LLC_BI__Account__c, LLC_BI__Due_Date__c, CCS_Action__c, CCS_Bank_Entity__c, CCS_Covenant_Type__c, CCS_Description__c, CCS_Entity_Tested__c, CCS_Grace_Period_Days__c, CCS_IsFacilityBooked__c, CCS_Measure__c, CCS_Period_End__c, CCS_Restricted_User__c, CCS_Rolling_12M__c, CCS_State__c, CCS_Status__c, CCS_Test_Status__c, CCS_Tested_Against__c, CCS_Value__c, Covenant_Mgmt__c, CCS_Defination__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant2_staging;



--$$$$$$$$$$$$$$$$$$$$$$$$$---compliance_staging--

SELECT
    Id  , IsDeleted  , Name  , CurrencyIsoCode  , SAFE_CAST(TIMESTAMP_MILLIS(CreatedDate) AS DATETIME) as CreatedDate  , CreatedById  ,  SAFE_CAST(TIMESTAMP_MILLIS(LastModifiedDate) AS DATETIME) as  LastModifiedDate  , LastModifiedById  , SAFE_CAST(TIMESTAMP_MILLIS(SystemModstamp) AS DATETIME) as SystemModstamp  ,SAFE_CAST(TIMESTAMP_MILLIS(LastActivityDate) AS DATE) LastActivityDate  , ConnectionReceivedId  , ConnectionSentId  , LLC_BI__Covenant__c  , SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Approval_Date__c) AS DATE)  as LLC_BI__Approval_Date__c  , LLC_BI__Approved_By__c  , LLC_BI__Comments__c  , LLC_BI__Evaluated_By__c  ,   SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Evaluation_Date__c) AS DATE) as  LLC_BI__Evaluation_Date__c  , SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Exception_Date__c) AS DATE) as  LLC_BI__Exception_Date__c  , LLC_BI__Status__c  , LLC_BI__Historic_Financial_Indicator__c  , LLC_BI__Associated_Spread_Statement_Period__c  , LLC_BI__Associated_Statement_Period_Status__c  , LLC_BI__Automated_Testing_Status__c  , LLC_BI__Bundle__c  ,  SAFE_CAST(TIMESTAMP_MILLIS(LLC_BI__Effective_Date__c) AS DATE) as LLC_BI__Effective_Date__c  , LLC_BI__Period_Key__c  , cm_Covenant_Compliance_Indicator_Value__c  , cm_Approver__c  , CCS_Action__c  , CCS_Denominator__c  , CCS_Frequency__c  , CCS_Numerator__c  , CCS_Outcome__c  ,     SAFE_CAST(TIMESTAMP_MILLIS(CCS_Period_End__c) AS DATE) as  CCS_Period_End__c  , CCS_Result__c  , CCS_Value__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_compliance2
except distinct
SELECT
    --ReplayId
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant__c, LLC_BI__Approval_Date__c, LLC_BI__Approved_By__c, LLC_BI__Comments__c, LLC_BI__Evaluated_By__c, LLC_BI__Evaluation_Date__c, LLC_BI__Exception_Date__c, LLC_BI__Status__c, LLC_BI__Historic_Financial_Indicator__c, LLC_BI__Associated_Spread_Statement_Period__c, LLC_BI__Associated_Statement_Period_Status__c, LLC_BI__Automated_Testing_Status__c, LLC_BI__Bundle__c, LLC_BI__Effective_Date__c, LLC_BI__Period_Key__c, cm_Covenant_Compliance_Indicator_Value__c, cm_Approver__c, CCS_Action__c, CCS_Denominator__c, CCS_Frequency__c, CCS_Numerator__c, CCS_Outcome__c, CCS_Period_End__c, CCS_Result__c, CCS_Value__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_compliance2_staging;


--$$$$$$$$$$$$$$$$$$$$$$$$$$$$ COVENANT_TYPE$$$$$$$$$$$$
SELECT
    Id   , OwnerId   , IsDeleted   , Name   , CurrencyIsoCode   , SAFE_CAST(TIMESTAMP_MILLIS(CreatedDate) AS DATETIME) as  CreatedDate   , CreatedById   , SAFE_CAST(TIMESTAMP_MILLIS(LastModifiedDate) AS DATETIME) as LastModifiedDate   , LastModifiedById   , SAFE_CAST(TIMESTAMP_MILLIS(SystemModstamp) AS DATETIME) as  SystemModstamp   , SAFE_CAST(TIMESTAMP_MILLIS(LastActivityDate) AS DATE) as  LastActivityDate   ,SAFE_CAST(TIMESTAMP_MILLIS(LastViewedDate) AS DATETIME) as  LastViewedDate   ,SAFE_CAST(TIMESTAMP_MILLIS(LastReferencedDate) AS DATETIME)  LastReferencedDate   , ConnectionReceivedId   , ConnectionSentId   , LLC_BI__Category__c   , LLC_BI__lookupKey__c   , LLC_BI__Is_Financial_Indicator__c   , CCS_Description__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type

EXCEPT DISTINCT

SELECT
    -- ReplayId
    Id, OwnerId, IsDeleted, Name, CurrencyIsoCode,CreatedDate, CreatedById,  LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Category__c, LLC_BI__lookupKey__c, LLC_BI__Is_Financial_Indicator__c, CCS_Description__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_covenant_type_staging;

--$$$$$$$$$$$$$$$$$$$$ Delete TEST

SCENERIO: 

SELECT
    'STAGING' AS LAYER,
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant2__c, LLC_BI__Account__c, LLC_BI__Contact__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_staging.rskcsp_ds_account_covenant_staging
WHERE ID ='a1e26000000uXJmAAM'
UNION ALL
--EXCEPT DISTINCT
SELECT
    'CURATED' AS LAYER,
    Id, IsDeleted, Name, CurrencyIsoCode, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, ConnectionReceivedId, ConnectionSentId, LLC_BI__Covenant2__c, LLC_BI__Account__c, LLC_BI__Contact__c
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_account_covenant_curated
WHERE ID ='a1e26000000uXJmAAM'

;

SELECT * FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_account_covenant
WHERE ID ='a1e26000000uXJmAAM'
;



