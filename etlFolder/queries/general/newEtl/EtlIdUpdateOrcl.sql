UPDATE etl_log_iud_row SET iseth = :iseth WHERE idrw IN (SELECT column_value FROM TABLE(:idrws))
