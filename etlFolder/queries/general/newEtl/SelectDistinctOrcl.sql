SELECT id, period, timeoper, oper FROM (
    SELECT id, period, timeoper, oper,
           ROW_NUMBER() OVER (PARTITION BY period, id ORDER BY timeoper DESC) rn
    FROM etl_log_iud_row
    WHERE idrw IN (SELECT column_value FROM TABLE(:idlogs))
)
WHERE rn = 1
ORDER BY period, id
