--------------------------------------------------------------------------------
-- Аудитные триггеры СПРАВОЧНИКОВ: etl_jobs.isokaudit = 0.
--
-- Справочник (etlFolder/SpTableName.d/*.json) переносится не по журналу
-- etl_log_iud_row, а по заданию: даг SpEtlNew (dags/SpDagNew.py) раз в 5 минут
-- берёт
--     SELECT DISTINCT tablename FROM etl_jobs WHERE isokaudit = 0
-- и гоняет ВСЕ линии, у которых `dependence` равна этому tablename. После
-- успешного переноса он же ставит isokaudit = 1.
--
-- Ноль в isokaudit ставит ТОЛЬКО триггер на ведущей таблице. Нет триггера —
-- справочник не перенесётся никогда, при этом ни одна задача не покраснеет:
-- дагу просто нечего делать. Именно поэтому конструктор проверяет такие
-- триггеры отдельно (вкладка «Триггеры», вид «аудитный»).
--
-- Что важно:
--   * литерал в WHERE — это `dependence` линии (по умолчанию метка ведущей,
--     у SPEINDEX1/SPEINDEX2 — общая SPEINDEX). Сравнение регистрозависимое;
--   * строка с таким tablename должна СУЩЕСТВОВАТЬ в etl_jobs: UPDATE по
--     несуществующей строке не делает ничего (см. finalOrcl.sql / finalPost.sql,
--     где такие строки заводятся);
--   * триггер намеренно «глупый» — ни period, ни данные строки ему не нужны.
--------------------------------------------------------------------------------

-- Шаблон. Подставить <TABLE> (ведущая) и <DEPENDENCE> (метка в etl_jobs).
CREATE OR REPLACE TRIGGER tr_<TABLE>_iud
BEFORE INSERT OR UPDATE OR DELETE ON <TABLE>
FOR EACH ROW
BEGIN
    UPDATE koknaev.etl_jobs
       SET isokaudit = 0, last_success_ts = sysdate
     WHERE tablename = '<DEPENDENCE>';
END;
/

-- Пример: справочник CALENDAR.
CREATE OR REPLACE TRIGGER tr_calendar_iud
BEFORE INSERT OR UPDATE OR DELETE ON CALENDAR
FOR EACH ROW
BEGIN
    UPDATE koknaev.etl_jobs
       SET isokaudit = 0, last_success_ts = sysdate
     WHERE tablename = 'CALENDAR';
END;
/

-- Завести строку задания, если её ещё нет (без неё UPDATE выше бесполезен):
--   INSERT INTO etl_jobs (tablename, period, last_success_ts, isokaudit)
--   SELECT 'CALENDAR', trunc(sysdate), sysdate, 0 FROM dual
--   WHERE NOT EXISTS (SELECT 1 FROM etl_jobs WHERE tablename = 'CALENDAR');
--   COMMIT;

-- Проверка: какие справочники сейчас ждут переноса.
--   SELECT tablename, isokaudit, last_success_ts FROM etl_jobs
--   WHERE isokaudit = 0 ORDER BY tablename;
