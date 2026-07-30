--------------------------------------------------------------------------------
-- EindexMO: подготовка ВЕДОМОЙ таблицы на Oracle (линия PG → Oracle,
-- тип «ETL-табл»).
--
-- Пункты ТЗ:
--   1.1  поле CREATEDATE типа DATE (на Oracle DATE = дата, время не пишем);
--   1.3  снять все триггеры IUD с ведомой — записи в неё кладёт ETL, журнал
--        etl_log_iud_row ведётся только на ведущей (PostgreSQL), иначе перенос
--        сам себе наплодит строк в журнале.
--
-- Ведущая часть (createdate, backfill, триггеры) —
-- etlFolder/queries/postgresSetup/01_eindexmo_etl_setup.sql.
--
-- Схему ниже выверить под реальную БД (в проекте ведомые лежат в KOKNAEV).
-- Запускать под владельцем таблицы: роли внутри PL/SQL не действуют.
--------------------------------------------------------------------------------

-- 1.1. Поле CREATEDATE ---------------------------------------------------------
-- Повторный запуск даст ORA-01430 (column already exists) — это нормально.
ALTER TABLE koknaev.eindexmo ADD (createdate DATE);

-- Индекс под выборку периодов ETL (DateSelectSlaveOrcl / DeletePeriodOrcl).
CREATE INDEX koknaev.ix_eindexmo_createdate ON koknaev.eindexmo (createdate);


-- 1.3. Снять все триггеры ------------------------------------------------------
-- Сначала посмотреть, что есть:
--   SELECT owner, trigger_name, trigger_type, triggering_event, status
--     FROM all_triggers
--    WHERE table_owner = 'KOKNAEV' AND table_name = 'EINDEXMO';
--
-- Затем удалить каждый:
--   DROP TRIGGER koknaev.<TRIGGER_NAME>;
--
-- Либо разом (выполнить и запустить полученные строки, либо раскомментировать
-- блок ниже — он удаляет всё, что висит на таблице):
--   SELECT 'DROP TRIGGER ' || owner || '.' || trigger_name || ';'
--     FROM all_triggers
--    WHERE table_owner = 'KOKNAEV' AND table_name = 'EINDEXMO';

-- BEGIN
--     FOR t IN (SELECT owner, trigger_name
--                 FROM all_triggers
--                WHERE table_owner = 'KOKNAEV' AND table_name = 'EINDEXMO') LOOP
--         EXECUTE IMMEDIATE 'DROP TRIGGER ' || t.owner || '.' || t.trigger_name;
--     END LOOP;
-- END;
-- /

-- Проверка: запрос к all_triggers должен вернуть 0 строк.
