-- Отметить обработанными записи журнала группы, существовавшие на момент
-- старта (idrw <= граница). Условие по периоду строит Python — см.
-- EtlStatusChangePost.sql про COALESCE и индекс.
UPDATE etl_log_iud_row SET isetl = 1
WHERE isetl = 0
  AND tablename = %(tablename)s
  AND {period_cond}
  AND idrw <= %(idrwBefore)s
