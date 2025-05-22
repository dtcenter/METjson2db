SELECT count(*)
FROM
    metplusdata._default.MET_parser AS c
WHERE ARRAY_LENGTH(object_pairs(c.data)) = 3
