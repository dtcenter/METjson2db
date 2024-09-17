SELECT
    c.ID,
    count(c.data) as cnt
FROM
    metdata._default.MET AS c
GROUP BY
    c.ID,
    c.data;