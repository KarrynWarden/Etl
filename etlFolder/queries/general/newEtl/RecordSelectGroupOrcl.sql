-- См. RecordSelectGroupPost.sql (в том числе про убранный LEFT JOIN etl_jobs).
SELECT {fields_str}
FROM ( {select_sql} ) p
WHERE {period_cond}
