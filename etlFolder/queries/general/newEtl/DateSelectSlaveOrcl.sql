SELECT {period_col}, MAX(lastupdate) AS lastupdate
FROM {tablename}
WHERE {filter}
GROUP BY {period_col}
