SELECT {fields_str}
FROM ( {select_sql} ) p
LEFT JOIN etl_jobs e ON e.tablename = %(tablename)s AND e.period = {period_expr}
WHERE {period_cond}
