UPDATE etl_log_iud_row SET isetl = 1
WHERE isetl = 0 AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = %(createdate)s AND tablename = %(tablename)s
