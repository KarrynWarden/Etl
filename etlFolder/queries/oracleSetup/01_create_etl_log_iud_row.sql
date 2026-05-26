--------------------------------------------------------------------------------
-- Создание таблицы etl_log_iud_row на Oracle.
--
-- Назначение: журнал INSERT/UPDATE/DELETE на ведущих таблицах. ETL-процесс
-- читает из этой таблицы строки с isetl = 0 и переносит соответствующие
-- записи в ведомую БД.
--
-- Поля совместимы с PostgreSQL-аналогом из etl_user.etl_log_iud_row.
--   tablename : имя группы (как в etl_jobs.tablename)
--   timeoper  : момент операции
--   oper      : 'IU' (insert+update объединены через MERGE) или 'D'
--   period    : значение createdate (или эквивалентного поля группировки)
--   id        : первичный ключ изменённой записи; для составного PK
--                сохраняется как 'pk1/pk2/...'
--   isetl     : 0 — требует обработки, 1 — обработано, -1 — ошибка ETL
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
    isetl     NUMBER(1)    DEFAULT 0
);

CREATE INDEX ix_etl_log_iud_row_tabl_isetl
    ON etl_log_iud_row(tablename, isetl);

CREATE INDEX ix_etl_log_iud_row_per
    ON etl_log_iud_row(period, id);


CREATE TABLE etl_log_iud_row (
    idrw      NUMBER       PRIMARY KEY, -- Убрали DEFAULT
    tablename VARCHAR2(100) NOT NULL,
    timeoper  TIMESTAMP    DEFAULT systimestamp,
    oper      VARCHAR2(2)  NOT NULL,
    period    DATE,
    id        VARCHAR2(200) NOT NULL,
    isetl     NUMBER(1)    DEFAULT 0
);

-- 3. Создаем триггер для автозаполнения ID
CREATE OR REPLACE TRIGGER trg_etl_log_iud_row_bi
BEFORE INSERT ON etl_log_iud_row
FOR EACH ROW
BEGIN
    IF :NEW.idrw IS NULL THEN
        SELECT etl_log_iud_row_seq.NEXTVAL INTO :NEW.idrw FROM DUAL;
    END IF;
END;
/

-- 4. Создаем индексы
CREATE INDEX ix_etl_log_iud_row_tabl_isetl ON etl_log_iud_row(tablename, isetl);
CREATE INDEX ix_etl_log_iud_row_per ON etl_log_iud_row(period, id);

grant select, insert, update, delete on etl_log_iud_row to etl_user



CREATE OR REPLACE TRIGGER tr_planoms_after_iud
AFTER INSERT OR UPDATE OR DELETE ON planoms
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.createdate;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('PLANOMS', systimestamp, p_oper, p_period, p_id, 0);
END;