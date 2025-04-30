SELECT
    DISTINCT RAW LINE_TYPE
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND VERSION = "{{vxVERSION}}"
    AND dataSetName = "{{vxDATASET}}"
    AND BMODEL = "{{vxMODEL}}"
order by
    LINE_TYPE