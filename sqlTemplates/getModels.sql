SELECT
    DISTINCT RAW AMODEL
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND dataSetName = "{{vxDATASET}}"
order by
    AMODEL