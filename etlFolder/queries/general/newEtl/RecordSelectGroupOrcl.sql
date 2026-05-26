SELECT {fields_str}
FROM ( {select_sql} ) p
LEFT JOIN koknaev.etl_jobs e ON e.tablename = :tablename AND e.period = p.{period_col}
WHERE {period_cond}
