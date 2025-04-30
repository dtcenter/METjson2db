SELECT
    DISTINCT RAW dataSetName
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND VERSION = "{{vxVERSION}}"
order by
    dataSetName