SELECT
    DISTINCT RAW dataSetName
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND LINE_TYPE = "{{vxLINETYPE}}"
order by
    dataSetName