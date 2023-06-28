
version: 2
sources:
  - name: downstream_raw
    database: "dmn01-rsksoi-bld-01-2017" # "{{ env_var('BQ_PROJECT_ID') }}"
    schema: "dmn01_rsksoi_euwe2_rsk_csp_downstream_raw" # "{{ env_var('BQ_RAW_DATASET_ID') }}"
    tables:
      - name: rskcsp_ds_covenant2
        description: Credit Suite Product, downstream raw table for covenant2. Covenant Mgmt
        columns:
          - name: Id
<<<<<<< Updated upstream
            description: Record ID
=======
            tests:
              - unique
>>>>>>> Stashed changes
            meta:
              data_type: STRING
          - name: OwnerId
            description: Owner ID
            meta:
              data_type: STRING
          - name: IsDeleted
            description: Deleted
            meta:
              data_type: STRING
          - name: Name
            description: Covenant Auto Number
            meta:
              data_type: STRING
          - name: CurrencyIsoCode
            description: Currency ISO Code
            meta:
              data_type: STRING
          - name: CreatedDate
            description: Created Date
            meta:
              data_type: DATETIME
          - name: CreatedById
            description: Created By ID
            meta:
              data_type: STRING
          - name: LastModifiedDate
            description: Last Modified Date
            meta:
              data_type: DATETIME
          - name: LastModifiedById
            description: Last Modified By ID
            meta:
              data_type: STRING
          - name: SystemModstamp
            description: System Modstamp
            meta:
              data_type: DATETIME
          - name: LastActivityDate
            description: Last Activity Date
            meta:
              data_type: DATE
          - name: LastViewedDate
            description: Last Viewed Date
            meta:
              data_type: DATETIME
          - name: LastReferencedDate
            description: Last Referenced Date
            meta:
              data_type: DATETIME
          - name: ConnectionReceivedId
            description: Received Connection ID
            meta:
              data_type: STRING
          - name: ConnectionSentId
            description: Sent Connection ID
            meta:
              data_type: STRING
          - name: LLC_BI__Active__c
            description: Active
            meta:
              data_type: STRING
          - name: LLC_BI__Compliance_Days_Prior__c
            description: Compliance Days Prior
            meta:
              data_type: STRING
          - name: LLC_BI__Covenant_Type__c
            description: Covenant Type
            meta:
              data_type: STRING
          - name: LLC_BI__Days_Past_Next_Evaluation__c
            description: Days Past Next Evaluation
            meta:
              data_type: NUMERIC
          - name: LLC_BI__Days_Until_Next_Evaluation__c
            description: Days Until Next Evaluation
            meta:
              data_type: NUMERIC
          - name: LLC_BI__Detail__c
            description: Detail
            meta:
              data_type: STRING
          - name: LLC_BI__Financial_Indicator_Value__c
            description: Financial Indicator Value
            meta:
              data_type: NUMERIC
          - name: LLC_BI__Frequency_Months__c
            description: Frequency Months
            meta:
              data_type: NUMERIC
          - name: LLC_BI__Frequency__c
            description: Frequency
            meta:
              data_type: STRING
          - name: LLC_BI__Is_Template__c
            description: Is_Template
            meta:
              data_type: STRING
          - name: LLC_BI__Last_Evaluation_Date__c
            description: Last Evaluation Date
            meta:
              data_type: DATE
          - name: LLC_BI__Last_Evaluation_Status__c
            description: Last Evaluation Status
            meta:
              data_type: STRING
          - name: LLC_BI__Next_Evaluation_Date__c
            description: Next Evaluation Date
            meta:
              data_type: DATE
          - name: LLC_BI__Notes__c
            description: Notes
            meta:
              data_type: STRING
          - name: LLC_BI__Required__c
            description: Required
            meta:
              data_type: STRING
          - name: LLC_BI__lookupKey__c
            description: lookupKey
            meta:
              data_type: STRING
          - name: Relationship__c
            description: Relationship
            meta:
              data_type: STRING
          - name: LLC_BI__Effective_Date__c
            description: Effective Date
            meta:
              data_type: DATE
          - name: LLC_BI__Linked_Spread_Statement_Record_Total__c
            description: Linked Spread Statement Record Total
            meta:
              data_type: STRING
          - name: LLC_BI__Linked_Spread_Statement_Record__c
            description: Linked Spread Statement Record
            meta:
              data_type: STRING
          - name: LLC_BI__Related_Covenant__c
            description: Related Covenant
            meta:
              data_type: STRING
          - name: Migration_ID__c
            description: Migration ID
            meta:
              data_type: STRING
          - name: Integration_Source__c
            description: Integration Source
            meta:
              data_type: STRING
          - name: LLC_BI__Comments__c
            description: Comments
            meta:
              data_type: STRING
          - name: isTest__c
            description: isTest
            meta:
              data_type: STRING
          - name: LLC_BI__Account__c
            description: Account
            meta:
              data_type: STRING
          - name: LLC_BI__Due_Date__c
            description: Due Date
            meta:
              data_type: DATE
          - name: CCS_Action__c
            description: Action
            meta:
              data_type: STRING
          - name: CCS_Bank_Entity__c
            description: Bank Entity
            meta:
              data_type: STRING
          - name: CCS_Covenant_Type__c
            description: Covenant Type
            meta:
              data_type: STRING
          - name: CCS_Description__c
            description: Description
            meta:
              data_type: STRING
          - name: CCS_Entity_Tested__c
            description: Entity Tested
            meta:
              data_type: STRING
          - name: CCS_Grace_Period_Days__c
            description: Grace Period Days
            meta:
              data_type: NUMERIC
          - name: CCS_IsFacilityBooked__c
            description: IsFacilityBooked
            meta:
              data_type: STRING
          - name: CCS_Measure__c
            description: Measure
            meta:
              data_type: STRING
          - name: CCS_Period_End__c
            description: Period End
            meta:
              data_type: DATE
          - name: CCS_Restricted_User__c
            description: Restricted User
            meta:
              data_type: STRING
          - name: CCS_Rolling_12M__c
            description: Rolling 12M
            meta:
              data_type: STRING
          - name: CCS_State__c
            description: State
            meta:
              data_type: STRING
          - name: CCS_Status__c
            description: Status
            meta:
              data_type: STRING
          - name: CCS_Test_Status__c
            description: Test Status
            meta:
              data_type: STRING
          - name: CCS_Tested_Against__c
            description: Tested Against
            meta:
              data_type: STRING
          - name: CCS_Value__c
            description: Value
            meta:
              data_type: STRING
          - name: Covenant_Mgmt__c
            description: Information Required
            meta:
              data_type: STRING
          - name: CCS_Defination__c
            description: Defination
            meta:
              data_type: STRING
          - name: ReplayId
            description: The replay id
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader
            description: The Change Event Header
            meta:
              data_type: RECORD
            tests:
              - not_null
          - name: ChangeEventHeader.entityName
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.recordIds
            description: One or more record IDs for the changed records. Typically, this field contains one record ID. If in one transaction the same change occurred in multiple records of the same object type during one second, Salesforce merges the change notifications. In this case, Salesforce sends one change event for all affected records and the recordIds field contains the IDs for all records that have the same change.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeType
            description: The operation that caused the change.Can be one of the following values - CREATE, UPDATE, DELETE, UNDELETE.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changedFields
            description: A list of the fields that were changed in an update operation, including the LastModifiedDate system field. This field is empty for other operations, including record creation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeOrigin
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.transactionKey
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.sequenceNumber
            description: The sequence of the change within a transaction. The sequence number starts from 1. A lead conversion is an example of a transaction that can have multiple changes. A lead conversion results in the following sequence of changes, all within the same transaction.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitTimestamp
            description: The date and time when the change occurred, represented as the number of milliseconds since January 1, 1970 00:00:00 GMT.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitUser
            description: The ID of the user that ran the change operation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.commitNumber
            description: The system change number (SCN) of a committed transaction, which increases sequentially. This field is provided for diagnostic purposes. The field value is not guaranteed to be unique in Salesforce—it is unique only in a single database instance. If your Salesforce org migrates to another database instance, the commit number might not be unique or sequential.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: _ObjectType
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: _EventType
            description: Encoded identifier for the EventType
            meta:
              data_type: STRING
            tests:
              - not_null

      - name: rskcsp_ds_account_covenant
        description: Credit Suite Product, downstream raw table for account_covenant. Relationship Covenant
        columns:
          - name: Id
            description: Record ID
            meta:
              data_type: STRING
          - name: IsDeleted
            description: Deleted
            meta:
              data_type: STRING
          - name: Name
            description: Covenant Number
            meta:
              data_type: STRING
          - name: CurrencyIsoCode
            description: Currency ISO Code
            meta:
              data_type: STRING
          - name: CreatedDate
            description: Created Date
            meta:
              data_type: DATETIME
          - name: CreatedById
            description: Created By ID
            meta:
              data_type: STRING
          - name: LastModifiedDate
            description: Last Modified Date
            meta:
              data_type: DATETIME
          - name: LastModifiedById
            description: Last Modified By ID
            meta:
              data_type: STRING
          - name: SystemModstamp
            description: System Modstamp
            meta:
              data_type: DATETIME
          - name: LastActivityDate
            description: Last Activity Date
            meta:
              data_type: DATE
          - name: ConnectionReceivedId
            description: Received Connection ID
            meta:
              data_type: STRING
          - name: ConnectionSentId
            description: Sent Connection ID
            meta:
              data_type: STRING
          - name: LLC_BI__Covenant2__c
            description: Covenant
            meta:
              data_type: STRING
          - name: LLC_BI__Account__c
            description: Relationship
            meta:
              data_type: STRING
          - name: LLC_BI__Contact__c
            description: Contact
            meta:
              data_type: STRING
          - name: ReplayId
            description: The replay id
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader
            description: The Change Event Header
            meta:
              data_type: RECORD
            tests:
              - not_null
          - name: ChangeEventHeader.entityName
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.recordIds
            description: One or more record IDs for the changed records. Typically, this field contains one record ID. If in one transaction the same change occurred in multiple records of the same object type during one second, Salesforce merges the change notifications. In this case, Salesforce sends one change event for all affected records and the recordIds field contains the IDs for all records that have the same change.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeType
            description: The operation that caused the change.Can be one of the following values - CREATE, UPDATE, DELETE, UNDELETE.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changedFields
            description: A list of the fields that were changed in an update operation, including the LastModifiedDate system field. This field is empty for other operations, including record creation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeOrigin
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.transactionKey
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.sequenceNumber
            description: The sequence of the change within a transaction. The sequence number starts from 1. A lead conversion is an example of a transaction that can have multiple changes. A lead conversion results in the following sequence of changes, all within the same transaction.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitTimestamp
            description: The date and time when the change occurred, represented as the number of milliseconds since January 1, 1970 00:00:00 GMT.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitUser
            description: The ID of the user that ran the change operation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.commitNumber
            description: The system change number (SCN) of a committed transaction, which increases sequentially. This field is provided for diagnostic purposes. The field value is not guaranteed to be unique in Salesforce—it is unique only in a single database instance. If your Salesforce org migrates to another database instance, the commit number might not be unique or sequential.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: _ObjectType
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: _EventType
            description: Encoded identifier for the EventType
            meta:
              data_type: STRING
            tests:
              - not_null

      - name: rskcsp_ds_covenant_compliance2
        description: Credit Suite Product, downstream raw table for covenant_compliance2. Covenant Compliance
        columns:
          - name: Id
            description: Record ID
            meta:
              data_type: STRING
          - name: IsDeleted
            description: Deleted
            meta:
              data_type: STRING
          - name: Name
            description: Covenant Compliance Number
            meta:
              data_type: STRING
          - name: CurrencyIsoCode
            description: Currency ISO Code
            meta:
              data_type: STRING
          - name: CreatedDate
            description: Created Date
            meta:
              data_type: DATETIME
          - name: CreatedById
            description: Created By ID
            meta:
              data_type: STRING
          - name: LastModifiedDate
            description: Last Modified Date
            meta:
              data_type: DATETIME
          - name: LastModifiedById
            description: Last Modified By ID
            meta:
              data_type: STRING
          - name: SystemModstamp
            description: System Modstamp
            meta:
              data_type: DATETIME
          - name: LastActivityDate
            description: Last Activity Date
            meta:
              data_type: DATE
          - name: ConnectionReceivedId
            description: Received Connection ID
            meta:
              data_type: STRING
          - name: ConnectionSentId
            description: Sent Connection ID
            meta:
              data_type: STRING
          - name: LLC_BI__Covenant__c
            description: Covenant
            meta:
              data_type: STRING
          - name: LLC_BI__Approval_Date__c
            description: Approval Date
            meta:
              data_type: DATE
          - name: LLC_BI__Approved_By__c
            description: Approved By
            meta:
              data_type: STRING
          - name: LLC_BI__Comments__c
            description: Comments
            meta:
              data_type: STRING
          - name: LLC_BI__Evaluated_By__c
            description: Evaluated By
            meta:
              data_type: STRING
          - name: LLC_BI__Evaluation_Date__c
            description: Evaluation Date
            meta:
              data_type: DATE
          - name: LLC_BI__Exception_Date__c
            description: Exception Date
            meta:
              data_type: DATE
          - name: LLC_BI__Status__c
            description: Status
            meta:
              data_type: STRING
          - name: LLC_BI__Historic_Financial_Indicator__c
            description: Historic Financial Indicator Value
            meta:
              data_type: NUMERIC
          - name: LLC_BI__Associated_Spread_Statement_Period__c
            description: Associated Spread Statement Period
            meta:
              data_type: STRING
          - name: LLC_BI__Associated_Statement_Period_Status__c
            description: Associated Statement Period Status
            meta:
              data_type: STRING
          - name: LLC_BI__Automated_Testing_Status__c
            description: Automated Testing Status
            meta:
              data_type: STRING
          - name: LLC_BI__Bundle__c
            description: Bundle
            meta:
              data_type: STRING
          - name: LLC_BI__Effective_Date__c
            description: Effective Date
            meta:
              data_type: DATE
          - name: LLC_BI__Period_Key__c
            description: Period Key
            meta:
              data_type: STRING
          - name: cm_Covenant_Compliance_Indicator_Value__c
            description: Covenant Compliance Indicator Value
            meta:
              data_type: STRING
          - name: cm_Approver__c
            description: Approver
            meta:
              data_type: STRING
          - name: CCS_Action__c
            description: Action
            meta:
              data_type: STRING
          - name: CCS_Denominator__c
            description: Denominator
            meta:
              data_type: NUMERIC
          - name: CCS_Frequency__c
            description: Frequency
            meta:
              data_type: STRING
          - name: CCS_Numerator__c
            description: Numerator
            meta:
              data_type: NUMERIC
          - name: CCS_Outcome__c
            description: Outcome
            meta:
              data_type: STRING
          - name: CCS_Period_End__c
            description: Period End
            meta:
              data_type: DATE
          - name: CCS_Result__c
            description: Result
            meta:
              data_type: NUMERIC
          - name: CCS_Value__c
            description: Value
            meta:
              data_type: NUMERIC
          - name: ReplayId
            description: The replay id
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader
            description: The Change Event Header
            meta:
              data_type: RECORD
            tests:
              - not_null
          - name: ChangeEventHeader.entityName
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.recordIds
            description: One or more record IDs for the changed records. Typically, this field contains one record ID. If in one transaction the same change occurred in multiple records of the same object type during one second, Salesforce merges the change notifications. In this case, Salesforce sends one change event for all affected records and the recordIds field contains the IDs for all records that have the same change.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeType
            description: The operation that caused the change.Can be one of the following values - CREATE, UPDATE, DELETE, UNDELETE.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changedFields
            description: A list of the fields that were changed in an update operation, including the LastModifiedDate system field. This field is empty for other operations, including record creation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeOrigin
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.transactionKey
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.sequenceNumber
            description: The sequence of the change within a transaction. The sequence number starts from 1. A lead conversion is an example of a transaction that can have multiple changes. A lead conversion results in the following sequence of changes, all within the same transaction.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitTimestamp
            description: The date and time when the change occurred, represented as the number of milliseconds since January 1, 1970 00:00:00 GMT.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitUser
            description: The ID of the user that ran the change operation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.commitNumber
            description: The system change number (SCN) of a committed transaction, which increases sequentially. This field is provided for diagnostic purposes. The field value is not guaranteed to be unique in Salesforce—it is unique only in a single database instance. If your Salesforce org migrates to another database instance, the commit number might not be unique or sequential.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: _ObjectType
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: _EventType
            description: Encoded identifier for the EventType
            meta:
              data_type: STRING
            tests:
              - not_null

      - name: rskcsp_ds_covenant_type
        description: Credit Suite Product, downstream raw table for covenant_type. Covenant Type
        columns:
          - name: Id
            description: Record ID
            meta:
              data_type: STRING
          - name: OwnerId
            description: Owner ID
            meta:
              data_type: STRING
          - name: IsDeleted
            description: Deleted
            meta:
              data_type: STRING
          - name: Name
            description: Covenant Type
            meta:
              data_type: STRING
          - name: CurrencyIsoCode
            description: Currency ISO Code
            meta:
              data_type: STRING
          - name: CreatedDate
            description: Created Date
            meta:
              data_type: DATETIME
          - name: CreatedById
            description: Created By ID
            meta:
              data_type: STRING
          - name: LastModifiedDate
            description: Last Modified Date
            meta:
              data_type: DATETIME
          - name: LastModifiedById
            description: Last Modified By ID
            meta:
              data_type: STRING
          - name: SystemModstamp
            description: System Modstamp
            meta:
              data_type: DATETIME
          - name: LastActivityDate
            description: Last Activity Date
            meta:
              data_type: DATE
          - name: LastViewedDate
            description: Last Viewed Date
            meta:
              data_type: DATETIME
          - name: LastReferencedDate
            description: Last Referenced Date
            meta:
              data_type: DATETIME
          - name: ConnectionReceivedId
            description: Received Connection ID
            meta:
              data_type: STRING
          - name: ConnectionSentId
            description: Sent Connection ID
            meta:
              data_type: STRING
          - name: LLC_BI__Category__c
            description: Category
            meta:
              data_type: STRING
          - name: LLC_BI__lookupKey__c
            description: lookupKey
            meta:
              data_type: STRING
          - name: LLC_BI__Is_Financial_Indicator__c
            description: Financial Indicator
            meta:
              data_type: STRING
          - name: CCS_Description__c
            description: Description
            meta:
              data_type: STRING
          - name: ReplayId
            description: The replay id
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader
            description: The Change Event Header
            meta:
              data_type: RECORD
            tests:
              - not_null
          - name: ChangeEventHeader.entityName
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.recordIds
            description: One or more record IDs for the changed records. Typically, this field contains one record ID. If in one transaction the same change occurred in multiple records of the same object type during one second, Salesforce merges the change notifications. In this case, Salesforce sends one change event for all affected records and the recordIds field contains the IDs for all records that have the same change.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeType
            description: The operation that caused the change.Can be one of the following values - CREATE, UPDATE, DELETE, UNDELETE.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changedFields
            description: A list of the fields that were changed in an update operation, including the LastModifiedDate system field. This field is empty for other operations, including record creation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeOrigin
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.transactionKey
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.sequenceNumber
            description: The sequence of the change within a transaction. The sequence number starts from 1. A lead conversion is an example of a transaction that can have multiple changes. A lead conversion results in the following sequence of changes, all within the same transaction.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitTimestamp
            description: The date and time when the change occurred, represented as the number of milliseconds since January 1, 1970 00:00:00 GMT.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitUser
            description: The ID of the user that ran the change operation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.commitNumber
            description: The system change number (SCN) of a committed transaction, which increases sequentially. This field is provided for diagnostic purposes. The field value is not guaranteed to be unique in Salesforce—it is unique only in a single database instance. If your Salesforce org migrates to another database instance, the commit number might not be unique or sequential.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: _ObjectType
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: _EventType
            description: Encoded identifier for the EventType
            meta:
              data_type: STRING
            tests:
              - not_null

      - name: rskcsp_ds_loan_covenant
        description: Credit Suite Product, downstream raw table for loan_covenant. Loan Covenant
        columns:
          - name: Id
            description: Record ID
            meta:
              data_type: STRING
          - name: IsDeleted
            description: Deleted
            meta:
              data_type: STRING
          - name: Name
            description: LoanCovenant Number
            meta:
              data_type: STRING
          - name: CurrencyIsoCode
            description: Currency ISO Code
            meta:
              data_type: STRING
          - name: CreatedDate
            description: Created Date
            meta:
              data_type: DATETIME
          - name: CreatedById
            description: Created By ID
            meta:
              data_type: STRING
          - name: LastModifiedDate
            description: Last Modified Date
            meta:
              data_type: DATETIME
          - name: LastModifiedById
            description: Last Modified By ID
            meta:
              data_type: STRING
          - name: SystemModstamp
            description: System Modstamp
            meta:
              data_type: DATETIME
          - name: LastActivityDate
            description: Last Activity Date
            meta:
              data_type: DATE
          - name: ConnectionReceivedId
            description: Received Connection ID
            meta:
              data_type: STRING
          - name: ConnectionSentId
            description: Sent Connection ID
            meta:
              data_type: STRING
          - name: LLC_BI__Covenant2__c
            description: Covenant2
            meta:
              data_type: STRING
          - name: LLC_BI__Loan__c
            description: Loan
            meta:
              data_type: STRING
          - name: LLC_BI__Active__c
            description: Active
            meta:
              data_type: STRING
          - name: CCS_UserProfile__c
            description: UserProfile
            meta:
              data_type: STRING
          - name: CCS_Restricted_User__c
            description: Restricted User
            meta:
              data_type: STRING
          - name: ReplayId
            description: The replay id
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader
            description: The Change Event Header
            meta:
              data_type: RECORD
            tests:
              - not_null
          - name: ChangeEventHeader.entityName
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.recordIds
            description: One or more record IDs for the changed records. Typically, this field contains one record ID. If in one transaction the same change occurred in multiple records of the same object type during one second, Salesforce merges the change notifications. In this case, Salesforce sends one change event for all affected records and the recordIds field contains the IDs for all records that have the same change.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeType
            description: The operation that caused the change.Can be one of the following values - CREATE, UPDATE, DELETE, UNDELETE.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changedFields
            description: A list of the fields that were changed in an update operation, including the LastModifiedDate system field. This field is empty for other operations, including record creation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.changeOrigin
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.transactionKey
            description: A string that uniquely identifies each Salesforce transaction. You can use this key to identify and group all changes that were made in the same transaction.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.sequenceNumber
            description: The sequence of the change within a transaction. The sequence number starts from 1. A lead conversion is an example of a transaction that can have multiple changes. A lead conversion results in the following sequence of changes, all within the same transaction.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitTimestamp
            description: The date and time when the change occurred, represented as the number of milliseconds since January 1, 1970 00:00:00 GMT.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: ChangeEventHeader.commitUser
            description: The ID of the user that ran the change operation.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: ChangeEventHeader.commitNumber
            description: The system change number (SCN) of a committed transaction, which increases sequentially. This field is provided for diagnostic purposes. The field value is not guaranteed to be unique in Salesforce—it is unique only in a single database instance. If your Salesforce org migrates to another database instance, the commit number might not be unique or sequential.
            meta:
              data_type: INTEGER
            tests:
              - not_null
          - name: _ObjectType
            description: The API name of the standard or custom object that the change pertains to. For example, Account or MyObject__c.
            meta:
              data_type: STRING
            tests:
              - not_null
          - name: _EventType
            description: Encoded identifier for the EventType
            meta:
              data_type: STRING
            tests:
              - not_null

