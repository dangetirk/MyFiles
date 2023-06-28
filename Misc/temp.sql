CREATE TABLE  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.newtable AS (

SELECT
ReplayId
,ChangeEventHeader
,ChangeEventHeader.entityName
,ChangeEventHeader.recordIds
,ChangeEventHeader.changeType
,ChangeEventHeader.changedFields
,ChangeEventHeader.changeOrigin
,ChangeEventHeader.transactionKey
,ChangeEventHeader.sequenceNumber
,ChangeEventHeader.commitTimestamp
,ChangeEventHeader.commitUser
,ChangeEventHeader.commitNumber
,Id
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type)
    OPTIONS(
               labels=[ ("owner", "vyrsk_data") ,
("cost_centre", "425482"),
("dataclassification","confidential"),
("component", "rsk-csp-metadata")]
    );

----------------
CREATE TABLE  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.newtable
    OPTIONS(
               labels=[ ("owner", "vyrsk_data") ,
("cost_centre", "425482"),
("dataclassification","confidential"),
("component", "rsk-csp-metadata")]
    )
AS (

SELECT
ReplayId
,ChangeEventHeader

,Id
FROM dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.rskcsp_ds_covenant_type)
