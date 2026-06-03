-- Реальные периоды строк ведомой по логическому id (idrw) — для режима
-- delete_insert. Берём ДО удаления, чтобы знать, какие группы etl_jobs
-- затронуты (period в etl_log_iud_row декоративный и здесь не используется).
-- Плейсхолдеры подставляются из do_etl (без фигурных скобок в комментарии):
--   period_expr  — колонка периода (или DATE()/TRUNC() при truncatePeriod);
--   tablename    — ведомая таблица;
--   primary_cond — idrw = :id0 [AND filterClauseSlave].
SELECT DISTINCT {period_expr} AS createdate
FROM {tablename}
WHERE {primary_cond}
