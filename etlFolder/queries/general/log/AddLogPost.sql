INSERT INTO etl_log (id_job, timeoper, status, errcode, errtext, action, tablename)
VALUES (%(ID_JOB)s, %(TIMEOPER)s, %(STATUS)s, %(ERRCODE)s, %(ERRTEXT)s, %(ACTION)s, %(TABLENAME)s)
