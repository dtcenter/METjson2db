SELECT
    DISTINCT RAW dataSetName
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
order by
    dataSetName