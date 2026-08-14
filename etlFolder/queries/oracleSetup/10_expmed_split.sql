--------------------------------------------------------------------------------
-- Разделение линии EXPMED на EXPMED23 (doctype 2,3) и EXPMED4 (doctype 4).
--
-- Зачем. У одной ведущей EXPMED две РАЗНЫЕ колонки периода: строки doctype 2,3
-- группируются по DOCEXPDT, строки doctype 4 — по DOCPENALTYDT. Период у линии
-- может быть только один, поэтому пока все три doctype жили в одной линии,
-- period в etl_log_iud_row для doctype=4 был декоративным: триггер писал туда
-- docexpdt, а группа считалась по docpenaltydt. Старая группа записи 4-го типа
-- находилась тогда только сравнением срезов, а у него есть слепое пятно — если
-- у среза не изменились ни MAX(lastupdate), ни количество строк, расхождения не
-- видно. После разделения у каждой линии свой честный период.
--
-- Триггер остаётся ОДИН и остаётся глупым: пишет по строке журнала на каждую
-- линию со своей колонкой периода, без разбора doctype. Никакого
-- «если doctype = 4, то docpenaltydt» в нём нет и не будет.
--
-- Порядок: 1) триггер, 2) журнал, 3) etl_jobs, 4) проверка.
--------------------------------------------------------------------------------

-- 1. Переустановить триггер (CREATE OR REPLACE — простоя нет).
--    Текст целиком: etlFolder/queries/triggers/EXPMEDOrclPost.sql
--    После установки он пишет уже две строки журнала на событие: одну с
--    tablename = 'EXPMED23' и period = docexpdt, вторую с 'EXPMED4' и
--    period = docpenaltydt.
@@../triggers/EXPMEDOrclPost.sql

-- 2. Закрыть старые записи журнала. Их некому обработать: линии EXPMED больше
--    нет, и они висели бы с isetl = 0 вечно.
--    Потери изменений здесь нет: режим section_compare_with_iud сверяет срезы
--    ведущей и ведомой на КАЖДОМ прогоне, журнал лишь ускоряет реакцию.
UPDATE koknaev.etl_log_iud_row
   SET isetl = 1
 WHERE tablename = 'EXPMED'
   AND isetl = 0;
COMMIT;

-- 3. etl_jobs заводить руками НЕ нужно: перенос сам регистрирует группу,
--    которой ещё нет (RegisterPeriod*). Первый прогон новых линий разложит
--    периоды по EXPMED23 и EXPMED4 сам.
--
--    Старые строки 'EXPMED' после этого ни на что не влияют (линии с таким
--    tablename нет), но и не мешают. Убрать их можно позже, когда станет видно,
--    что новые линии работают:
-- DELETE FROM koknaev.etl_jobs WHERE tablename = 'EXPMED';
-- COMMIT;

-- 4. Проверка. Сразу после установки триггера сделать любое изменение и
--    убедиться, что в журнале появились ОБЕ строки с разными периодами:
--
--    UPDATE koknaev.expmed SET docexpdt = docexpdt WHERE idrw = <любой>;
--    COMMIT;
SELECT tablename, oper, period, id, isetl, timeoper
  FROM koknaev.etl_log_iud_row
 WHERE tablename IN ('EXPMED23', 'EXPMED4')
 ORDER BY idrw DESC
 FETCH FIRST 10 ROWS ONLY;

-- Разошлись ли линии по группам так, как ожидается: у EXPMED23 периоды должны
-- совпадать с docexpdt, у EXPMED4 — с docpenaltydt.
SELECT j.tablename, COUNT(*) AS rows_,
       MIN(j.period) AS period_min, MAX(j.period) AS period_max
  FROM koknaev.etl_log_iud_row j
 WHERE j.tablename IN ('EXPMED23', 'EXPMED4')
 GROUP BY j.tablename;
