--------------------------------------------------------------------------------
-- Создание таблицы etl_log_iud_row на Oracle.
--
-- Назначение: журнал INSERT/UPDATE/DELETE на ведущих таблицах. ETL-процесс
-- читает из этой таблицы строки с iseth = 0 и переносит соответствующие
-- записи в ведомую БД.
--
-- Поля совместимы с PostgreSQL-аналогом из etl_user.etl_log_iud_row.
--   tablename : имя группы (как в etl_jobs.tablename)
--   timeoper  : момент операции
--   oper      : 'IU' (insert+update объединены через MERGE) или 'D'
--   period    : значение createdate (или эквивалентного поля группировки)
--   id        : первичный ключ изменённой записи; для составного PK
--                сохраняется как 'pk1/pk2/...'
--   iseth     : 0 — требует обработки, 1 — обработано, -1 — ошибка ETL
--   idrw      : суррогатный ключ
--------------------------------------------------------------------------------

CREATE SEQUENCE etl_log_iud_row_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE etl_log_iud_row (
    idrw      NUMBER       DEFAULT etl_log_iud_row_seq.NEXTVAL PRIMARY KEY,
    tablename VARCHAR2(100) NOT NULL,
    timeoper  TIMESTAMP    DEFAULT systimestamp,
    oper      VARCHAR2(2)  NOT NULL,
    period    DATE,
    id        VARCHAR2(200) NOT NULL,
    iseth     NUMBER(1)    DEFAULT 0
);

CREATE INDEX ix_etl_log_iud_row_tabl_iseth
    ON etl_log_iud_row(tablename, iseth);

CREATE INDEX ix_etl_log_iud_row_per
    ON etl_log_iud_row(period, id);
