--------------------------------------------------------------------------------
-- Создание таблицы etl_log_iud_row на Oracle.
--
-- Назначение: журнал INSERT/UPDATE/DELETE на ведущих таблицах. ETL-процесс
-- читает из этой таблицы строки с isetl = 0 и переносит соответствующие
-- записи в ведомую БД.
--
-- Поля:
--   idrw      : суррогатный ключ (PRIMARY KEY), заполняется из
--               etl_log_iud_row_seq BEFORE-INSERT триггером ниже;
--   tablename : имя группы (как в etl_jobs.tablename);
--   timeoper  : момент операции;
--   oper      : 'IU' (insert+update объединены через MERGE) или 'D';
--   period    : значение createdate (или эквивалентного поля группировки).
--               ВНИМАНИЕ: для режима delete_insert (expmed) это поле
--               ДЕКОРАТИВНОЕ — реальный период ETL берёт из данных, см. README;
--   id        : первичный ключ изменённой записи; для составного PK
--               сохраняется как 'pk1/pk2/...';
--   isetl     : 0 — требует обработки, 1 — обработано, -1 — ошибка ETL.
--
-- ВАЖНО (ORA-04098). Последовательность и триггер должны жить в ОДНОЙ схеме
-- с таблицей. Если триггер trg_etl_log_iud_row_bi не видит
-- etl_log_iud_row_seq (последовательности нет / она в другой схеме), он
-- становится INVALID и любой INSERT в etl_log_iud_row падает с ORA-04098
-- (а вслед за ним — триггеры ведущих таблиц, tr_<table>_after_iud).
-- Диагностика:
--   ALTER TRIGGER trg_etl_log_iud_row_bi COMPILE;
--   SELECT text FROM user_errors WHERE name='TRG_ETL_LOG_IUD_ROW_BI';
-- Если в ошибке PLS-00103 «end-of-file» — тело триггера залито ОБРЕЗАННЫМ:
-- клиент порезал PL/SQL-блок по внутреннему ';'. Пересоздать триггер ЦЕЛИКОМ
-- (одним блоком, вместе с '/'), а не построчно. На Oracle 12c+ можно вообще
-- без триггера: ALTER TABLE etl_log_iud_row MODIFY idrw
-- DEFAULT etl_log_iud_row_seq.NEXTVAL; затем DROP TRIGGER trg_etl_log_iud_row_bi.
--------------------------------------------------------------------------------

-- 1. Последовательность для idrw.
--    Если таблица уже содержит данные — START WITH должен быть ВЫШЕ
--    текущего MAX(idrw): SELECT NVL(MAX(idrw),0)+1 FROM etl_log_iud_row;
CREATE SEQUENCE etl_log_iud_row_seq START WITH 1 INCREMENT BY 1 NOCACHE;

-- 2. Таблица.
CREATE TABLE etl_log_iud_row (
    idrw      NUMBER        PRIMARY KEY,
    tablename VARCHAR2(100) NOT NULL,
    timeoper  TIMESTAMP     DEFAULT systimestamp,
    oper      VARCHAR2(2)   NOT NULL,
    period    DATE,
    id        VARCHAR2(200) NOT NULL,
    isetl     NUMBER(1)     DEFAULT 0
);

-- 3. Автозаполнение idrw из последовательности (совместимо со старыми
--    версиями Oracle без identity/DEFAULT seq.NEXTVAL).
CREATE OR REPLACE TRIGGER trg_etl_log_iud_row_bi
BEFORE INSERT ON etl_log_iud_row
FOR EACH ROW
BEGIN
    IF :NEW.idrw IS NULL THEN
        SELECT etl_log_iud_row_seq.NEXTVAL INTO :NEW.idrw FROM DUAL;
    END IF;
END;
/

-- 4. Индексы под выборку ETL-процессом.
CREATE INDEX ix_etl_log_iud_row_tabl_isetl ON etl_log_iud_row(tablename, isetl);
CREATE INDEX ix_etl_log_iud_row_per ON etl_log_iud_row(period, id);

-- 5. Доступ ETL-пользователю (если журнал читается из-под отдельной учётки).
GRANT SELECT, INSERT, UPDATE, DELETE ON etl_log_iud_row TO etl_user;
