SELECT
    c.ID,
    ARRAY_LENGTH(object_pairs(c.data)) as data_length,
    object_pairs(c.data) [*].name as data_keys
FROM
    metdata._default.MET AS c
GROUP BY
    c.ID,
    c.data
ORDER BY
    data_length DESC
LIMIT 100