SELECT
    DISTINCT RAW AMODEL
FROM
    {{vxDBTARGET}}
WHERE
    type = "DD"
    AND subtype = "{{vxSUBTYPE}}"
    AND VERSION = "{{vxVERSION}}"
    AND dataSetName = "{{vxDATASET}}"
order by
    AMODEL