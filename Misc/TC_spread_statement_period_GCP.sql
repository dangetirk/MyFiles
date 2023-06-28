SELECT  
CAST(	Id	 AS STRING) AS 	Id
,CAST(	LLC_BI__Analyst__c	 AS STRING) AS 	LLC_BI__Analyst__c
,CAST(	LLC_BI__Average_Exchange_Rate__c	 AS STRING) AS 	LLC_BI__Average_Exchange_Rate__c
,CAST(	LLC_BI__Collateral_Column_Title__c	 AS STRING) AS 	LLC_BI__Collateral_Column_Title__c
,CAST(	CreatedById	 AS STRING) AS 	CreatedById
,CONCAT(REPLACE(CAST(CreatedDate AS STRING ),' ','T'),'.000+0000') AS CreatedDate
,CAST(	CurrencyIsoCode	 AS STRING) AS 	CurrencyIsoCode
,CAST(	LLC_BI__Data_Source__c	 AS STRING) AS 	LLC_BI__Data_Source__c
,CAST(	CCS_DatePeriodsSource__c	 AS STRING) AS 	CCS_DatePeriodsSource__c
,CAST(	LLC_BI__Debt_Schedule__c	 AS STRING) AS 	LLC_BI__Debt_Schedule__c
,CAST(	LLC_BI__Name_Override__c	 AS STRING) AS 	LLC_BI__Name_Override__c
,CAST(	LLC_BI__Exchange_Rate__c	 AS STRING) AS 	LLC_BI__Exchange_Rate__c
,CAST(	LLC_BI__External_Data_Source_Id__c	 AS STRING) AS 	LLC_BI__External_Data_Source_Id__c
,CAST(	LLC_BI__External_Period_Key__c	 AS STRING) AS 	LLC_BI__External_Period_Key__c
,CAST(	LLC_BI__externalLookupKey__c	 AS STRING) AS 	LLC_BI__externalLookupKey__c
,CAST(	LLC_BI__Fiscal_Year_TTM_Period__c	 AS STRING) AS 	LLC_BI__Fiscal_Year_TTM_Period__c
,CAST(	LLC_BI__Initial_Interim_TTM_Period__c	 AS STRING) AS 	LLC_BI__Initial_Interim_TTM_Period__c
,CAST(	LLC_BI__Is_Annual__c	 AS STRING) AS 	LLC_BI__Is_Annual__c
,CAST(	LLC_BI__Is_Fiscal_Year__c	 AS STRING) AS 	LLC_BI__Is_Fiscal_Year__c
,CAST(	LLC_BI__Is_Flex_Enabled_Debt_Schedule__c	 AS STRING) AS 	LLC_BI__Is_Flex_Enabled_Debt_Schedule__c
,CAST(	LLC_BI__Is_Global_Analysis_Year__c	 AS STRING) AS 	LLC_BI__Is_Global_Analysis_Year__c
,CAST(	LastModifiedById	 AS STRING) AS 	LastModifiedById
,CONCAT(REPLACE(CAST(LastModifiedDate AS STRING ),' ','T'),'.000+0000') AS LastModifiedDate
,CAST(	LLC_BI__Month__c	 AS STRING) AS 	LLC_BI__Month__c
,CAST(	LLC_BI__Number_of_Periods__c	 AS STRING) AS 	LLC_BI__Number_of_Periods__c
,CAST(	LLC_BI__Period_Key__c	 AS STRING) AS 	LLC_BI__Period_Key__c
,CAST(	LLC_BI__Project_from_Period__c	 AS STRING) AS 	LLC_BI__Project_from_Period__c
,CAST(	LLC_BI__Selected__c	 AS STRING) AS 	LLC_BI__Selected__c
,CAST(	LLC_BI__Selected_In_Global__c	 AS STRING) AS 	LLC_BI__Selected_In_Global__c
,CAST(	LLC_BI__Source__c	 AS STRING) AS 	LLC_BI__Source__c
,CAST(	LLC_BI__Source_Currency__c	 AS STRING) AS 	LLC_BI__Source_Currency__c
,CAST(	LLC_BI__Spread_Projections_Template__c	 AS STRING) AS 	LLC_BI__Spread_Projections_Template__c
,CAST(	Name	 AS STRING) AS 	Name
,CAST(	LLC_BI__Spread_Statement_Type__c	 AS STRING) AS 	LLC_BI__Spread_Statement_Type__c
,CAST(	LLC_BI__Statement_Date__c	 AS STRING) AS 	LLC_BI__Statement_Date__c
,CAST(	LLC_BI__Supplemental_Number_of_Periods__c	 AS STRING) AS 	LLC_BI__Supplemental_Number_of_Periods__c
,CAST(	LLC_BI__Supplemental_Source__c	 AS STRING) AS 	LLC_BI__Supplemental_Source__c
,CAST(	LLC_BI__Supplemental_Statement_Date__c	 AS STRING) AS 	LLC_BI__Supplemental_Statement_Date__c
,CAST(	LLC_BI__Trailing_Interim_TTM_Period__c	 AS STRING) AS 	LLC_BI__Trailing_Interim_TTM_Period__c
,CAST(	LLC_BI__Type__c	 AS STRING) AS 	LLC_BI__Type__c
,CAST(	LLC_BI__Unmapped_Values__c	 AS STRING) AS 	LLC_BI__Unmapped_Values__c
,CAST(	LLC_BI__Year__c	 AS STRING) AS 	LLC_BI__Year__c
,CAST(	LLC_BI__Year_Hidden_In_Global__c	 AS STRING) AS 	LLC_BI__Year_Hidden_In_Global__c

FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_ds_curation.rskcsp_ds_spread_statement_period_curated