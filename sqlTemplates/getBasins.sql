SELECT
    DISTINCT RAW BASIN
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND dataSetName = "{{vxDATASET}}"
    AND AMODEL = "{{vxMODEL}}"
    AND LINE_TYPE = "{{vxLINE_TYPE}}"
order by
    BASIN