UPDATE etl_jobs
SET isokaudit = %(ISOKAUDIT)s, last_success_ts = NULL
WHERE tablename = %(tablename)s AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = %(PERIOD)s
