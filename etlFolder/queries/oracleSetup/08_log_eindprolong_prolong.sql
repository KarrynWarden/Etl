--------------------------------------------------------------------------------
-- LOG_EINDPROLONG: журнал заявок на продление остаётся ТОЛЬКО на Oracle.
--
-- Было: линия LogeindprolongOrclPost возила koknaev.LOG_EINDPROLONG на Postgres,
-- там процедура pck_eindexmo.scanlogprolong разбирала копию и туда же писала
-- dwork/status. Копия нужна была исключительно процедуре.
--
-- Стало: даг A61ProceduresScanLogProlong читает заявки прямо с Oracle, зовёт
-- pck_eindexmo.ProlongDestrEindex на Postgres и проставляет итог обратно в
-- Oracle. Переноса больше нет, поэтому здесь два изменения на стороне Oracle:
--   1. etl_user должен уметь ОБНОВЛЯТЬ журнал (раньше хватало select);
--   2. триггер журнала изменений на LOG_EINDPROLONG больше не нужен — иначе он
--      продолжит копить строки в etl_log_iud_row, которые никто не разбирает
--      (а даг своими UPDATE'ами будет их ещё и плодить).
--
-- Запускать под владельцем таблицы (KOKNAEV).
--------------------------------------------------------------------------------

-- 1. Права на журнал -----------------------------------------------------------
-- Даг ставит dwork/status/lastupdate — нужен UPDATE. SELECT, скорее всего, уже
-- выдан (по нему работал перенос), повторная выдача безвредна.
GRANT SELECT, UPDATE ON koknaev.LOG_EINDPROLONG TO etl_user;


-- 2. Снять триггер журнала изменений -------------------------------------------
-- Проверить, что есть:
--   SELECT owner, trigger_name, triggering_event, status
--     FROM all_triggers
--    WHERE table_owner = 'KOKNAEV' AND table_name = 'LOG_EINDPROLONG';
--
-- Снимается только триггер ETL. LOGEINDPROLONG_INSERT_TRIGGER (idlog из
-- последовательности, createdate/lastupdate при вставке) — ТРОГАТЬ НЕЛЬЗЯ, он
-- часть самой таблицы, а не переноса.
DROP TRIGGER koknaev.tr_log_eindprolong_after_iud;


-- 3. Убрать хвосты переноса (по желанию, после снятия триггера) -----------------
-- Неразобранные строки журнала изменений и запись линии в etl_jobs больше не
-- нужны — новых не появится, старые просто занимают место.
--   DELETE FROM koknaev.etl_log_iud_row WHERE tablename = 'LOG_EINDPROLONG';
--   DELETE FROM koknaev.etl_jobs        WHERE tablename = 'LOG_EINDPROLONG';
--   COMMIT;
