INSERT INTO {tablename} ({columns_str})
VALUES ({values_str})
ON CONFLICT ({primary_str})
DO UPDATE SET {update_str}
