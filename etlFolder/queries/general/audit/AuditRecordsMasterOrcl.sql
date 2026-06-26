-- Записи ведущего источника за один период (createdate).
-- select_sql уже содержит при необходимости filterClause (doctype-срез).
-- period_cond строится в do_audit и содержит плейсхолдер createdate.
SELECT {fields_str}
FROM ( {select_sql} ) p
WHERE {period_cond}