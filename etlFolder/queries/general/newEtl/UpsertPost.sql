INSERT INTO {tablename} ({columns_str})
VALUES ({values_str})
ON CONFLICT ({primary_str}) {conflict_where}
DO UPDATE SET {update_str}
