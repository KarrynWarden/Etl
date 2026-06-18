-- id, idrw + поля для дедупликации в do_etl (_selectIudWork): один запрос
-- вместо связки SelectEtlIud + SelectDistinct (убирает второй запрос и
-- IN-список -> нет ORA-01795 при тысячах изменений).
SELECT id, idrw, period, timeoper, oper
FROM koknaev.etl_log_iud_row
WHERE isetl = 0 AND tablename = :tablename
ORDER BY timeoper
