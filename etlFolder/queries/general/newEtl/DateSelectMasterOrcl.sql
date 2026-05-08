SELECT {period_col} AS createdate, MAX(lastupdate) AS lastupdate
FROM ( {select_sql} ) p
GROUP BY {period_col}
