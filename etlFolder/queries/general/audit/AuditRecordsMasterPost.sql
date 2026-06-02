-- Записи ведущего источника за один период.
-- Плейсхолдеры подставляются из do_audit (имена без фигурных скобок,
-- чтобы str.format не трогал их в комментарии):
--   fields_str  — поля аудита;
--   select_sql  — источник, уже с filterClause (doctype-срез);
--   period_cond — условие по периоду с биндом %(createdate)s.
SELECT {fields_str}
FROM ( {select_sql} ) p
WHERE {period_cond}
