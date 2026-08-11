-- Записать результат аудита группы в etl_jobs.
--   1  — идентично;  -4 — есть отличия;  -2 — ошибка при проверке.
-- last_success_ts не трогаем — это поле переноса, не аудита.
UPDATE koknaev.etl_jobs
SET isokaudit = :ISOKAUDIT
WHERE tablename = :TABLENAME
  AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = :PERIOD