-- Записи ведомой таблицы за один период.
-- Плейсхолдеры подставляются из do_audit (имена без фигурных скобок,
-- чтобы str.format не трогал их в комментарии):
--   fields_str  — поля аудита;
--   tablename   — имя ведомой таблицы;
--   period_cond — условие по периоду (бинд :createdate и, при наличии,
--                 filterClauseSlave — тот же doctype-срез, что и при переносе).
SELECT {fields_str}
FROM {tablename} p
WHERE {period_cond}
