-- См. EtlStatusChangePost.sql: условие по периоду строит Python, чтобы оно не
-- прятало колонку под COALESCE (иначе индекс по периоду не применяется).
UPDATE koknaev.etl_log_iud_row SET isetl = 1
WHERE isetl = 0 AND {period_cond} AND tablename = :tablename
