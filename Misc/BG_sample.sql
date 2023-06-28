CREATE TABLE  dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_downstream_raw.newtable
(
    ID INT64 OPTIONS(description="An optional INTEGER field"),
    EVENT STRUCT<
    TEST ARRAY<STRING> OPTIONS(description="A repeated STRING field"),
    istype BOOL

    )

    OPTIONS(
labels=[ ("owner", "vyrsk_data") ,
("cost_centre", "425482"),
("dataclassification","confidential"),
("component", "rsk-csp-metadata")]
    )

    INSERT INTO `<GCP_PROJECT_NAME>.Test_Tables.Nested_Person_Table2` (id,

                                                                       address.status,
                                                                       address.address,
                                                                       address.city,
                                                                       address.state,
                                                                       address.zip,
                                                                       address.numberOfYears)
SELECT
    id,

    address.status,
    address.address,
    address.city,
    address.state,
    address.zip,
    address.numberOfYears
FROM
    `<GCP_PROJECT_NAME>.Test_Tables.Nested_Person_Table`