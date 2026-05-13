UPDATE etl_log_iud_row SET isetl = :isetl WHERE idrw IN (SELECT column_value FROM TABLE(:idrws))
