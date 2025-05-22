SELECT
    DISTINCT RAW LINE_TYPE
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND dataSetName = "{{vxDATASET}}"
    AND AMODEL = "{{vxMODEL}}"
order by
    LINE_TYPE