-- periodsSql для линии MEDREE_PRDISPOrclPost (режим query_section).
-- Выполняется на ВЕДУЩЕЙ (Oracle) и возвращает пары (year, month) — группы,
-- которые ETL перезальёт в Postgres полной перезаписью (DELETE группы + заливка).
--
-- Логика симметрична ночному Oracle-джобу (06_medree_prdisp_daily_job.sql): берём
-- периоды, обновлённые за последний месяц (по medcheck.lastupdate). ETL-перенос
-- ставим в расписании ПОСЛЕ джоба, чтобы переносить уже пересчитанные группы.
--
-- Порядок колонок важен: первая — year, вторая — month (ETL читает по позиции).
--
-- ПЕРВИЧНЫЙ полный перенос (таблица в Postgres пустая): временно замените запрос на
--     SELECT DISTINCT year, month FROM koknaev.medree_prdisp
-- прогоните линию один раз, затем верните регулярный вариант ниже.
SELECT DISTINCT c.year, c.month
FROM   medcheck c
WHERE  c.lastupdate >= ADD_MONTHS(TRUNC(SYSDATE), -1)
