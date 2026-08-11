SELECT {fields_str}
FROM ( {select_sql} ) p
LEFT JOIN koknaev.etl_jobs e ON e.tablename = :tablename
  AND COALESCE(e.period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = COALESCE({period_expr}, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
WHERE {period_cond}
