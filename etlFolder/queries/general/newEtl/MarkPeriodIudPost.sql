UPDATE etl_log_iud_row SET isetl = 1
WHERE isetl = 0
  AND tablename = %(tablename)s
  AND period = %(period)s
  AND idrw <= %(idrwBefore)s
