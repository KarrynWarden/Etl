MERGE INTO {tablename} USING dual
ON ({primary_cond})
WHEN MATCHED THEN
    UPDATE SET {update_str}
WHEN NOT MATCHED THEN
    INSERT ({insert_fields_str})
    VALUES ({insert_values_str})
