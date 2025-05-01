SELECT
    DISTINCT RAW STORM_ID
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
order by
    STORM_ID