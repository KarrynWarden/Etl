SELECT id, period, timeoper, oper FROM (
    SELECT id, period, timeoper, oper,
           ROW_NUMBER() OVER (PARTITION BY period, id ORDER BY timeoper DESC) rn
    FROM koknaev.etl_log_iud_row
    WHERE idrw IN ({in_clause})
)
WHERE rn = 1
ORDER BY period, id
