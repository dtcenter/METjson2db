SELECT STORM_ID,
       STORM_NAME,
       BMODEL,
       DESCR,
       VALID,
       data AS data
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND VERSION = "{{vxVERSION}}"
    AND dataSetName = "{{vxDATASET}}"
    AND AMODEL = "{{vxMODEL}}"
    AND LINE_TYPE = "{{vxLINE_TYPE}}"
    AND BASIN = "{{vxBASIN}}"
    AND STORM_ID IN {{vxSTORM_IDS_LIST}}

