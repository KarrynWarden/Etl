SELECT c.column_name, c.data_type, c.numeric_scale AS data_scale,
       CASE WHEN k.column_name IS NOT NULL THEN 'Primary Key' ELSE NULL END AS is_primary_key
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage k
       ON c.table_name = k.table_name
      AND c.column_name = k.column_name
      AND c.table_schema = k.table_schema
      AND k.constraint_name IN (
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE table_name = %(TABLENAME)s
            AND constraint_type = 'PRIMARY KEY'
      )
WHERE c.table_name = %(TABLENAME)s
ORDER BY c.column_name
