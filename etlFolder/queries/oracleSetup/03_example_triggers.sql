--------------------------------------------------------------------------------
-- Примеры готовых триггеров на основе шаблона 02_trigger_template.sql.
--
-- ВАЖНО ПРО ЛИНИИ НА СВОЁМ SELECT (mocheck и подобные). В конфиге такой линии
-- periodColumn и PK — это ПСЕВДОНИМЫ ЗАПРОСА, а не колонки ведущей таблицы:
--   ... dschet createdate ... FROM TFINSCHET   -> в триггере :new.dschet;
--   ... id            (idrw) ... FROM PODCHECK -> в триггере :new.id;
--   ... TO_DATE(YEAR||'-01-01','YYYY-MM-DD') createdate ...
-- Колонок CREATEDATE и IDRW в PODCHECK нет вовсе. Поэтому «колонка периода
-- CREATEDATE в теле не упоминается» для таких линий — не ошибка триггера, а
-- следствие того, что имя пришло из запроса. Конструктор (вкладка «Триггеры» и
-- кнопка «Предположить по SQL») разбирает запрос и подставляет настоящие
-- колонки сам; руками — смотреть в свой selectSql.
--
-- 1) Простая таблица medcheck (поле периода — UPDT, PK — IDRW).
-- 2) reqprepmo, которая участвует одновременно в двух направлениях
--    (orcl<->post и post<->post в роли mocheck) — два отдельных триггера
--    с разными tablename.
-- 3) mocheck-style: одна таблица, разделяемая по doctype, требует только
--    один триггер; разделение по группам берёт на себя селект-источник.
-- 4) podcheck: обратный случай — НЕСКОЛЬКО ведущих таблиц (PODCHECK и
--    SPPODNORM) кормят ОДНУ логическую линию, поэтому оба триггера пишут
--    ОДИН И ТОТ ЖЕ tablename.
--------------------------------------------------------------------------------

-- 1. medcheck
CREATE OR REPLACE TRIGGER tr_medcheck_after_iud
AFTER INSERT OR UPDATE OR DELETE ON medcheck
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.updt;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.updt;
        IF :old.idrw <> :new.idrw THEN
            INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('medcheck', systimestamp, 'D', :old.updt, TO_CHAR(:old.idrw), 0);
        END IF;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.updt;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('medcheck', systimestamp, p_oper, p_period, p_id, 0);
END;
/

-- 2. reqprepmo (два триггера — для каждого направления одна запись в журнал
--    под своим tablename; ETL-процессы читают разные ключи и не мешают друг
--    другу)
CREATE OR REPLACE TRIGGER tr_reqprepmo_after_iud
AFTER INSERT OR UPDATE OR DELETE ON reqprepmo
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.reqdt;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.reqdt;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.reqdt;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('reqprepmo', systimestamp, p_oper, p_period, p_id, 0);
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('reqprepmomocheck', systimestamp, p_oper, p_period, p_id, 0);
END;
/

-- 4. podcheck: линия PODCHECK (mocheck doctype 5 и 6), режим delete_insert.
--
-- Заменяет прежнюю пару линий PODCHECK3 (doctype=5) / PODCHECK4 (doctype=6).
-- Причина слияния: строка PODCHECK переезжает между typehelp=3 и typehelp=4,
-- то есть между doctype 5 и 6. Раздельные линии видели только свой doctype и
-- оставляли в mocheck осиротевшую строку в прежнем — ровно та же болезнь, из-за
-- которой на delete_insert перевели expmed.
--
-- ВАЖНО, ЧТО ИМЕННО ДОЛЖЕН ПИСАТЬ ТРИГГЕР:
--   tablename = 'PODCHECK' — РОВНО так, ВЕРХНИЙ регистр (сравнение в SQL
--     переноса регистрозависимое; значение = tableNameEtlJobs из
--     MOCHECK_LOGICAL_GROUPS в dags/MocheckOrclPost.py);
--   id        = idrw логической строки mocheck, то есть то, что в MOCHECK.sql
--     подставлено в колонку idrw: для PODCHECK это ID, для SPPODNORM — IDRW;
--   period    — ДЕКОРАТИВНЫЙ. В delete_insert он не участвует в логике:
--     реальные затронутые группы etl_jobs берутся из данных (для удаляемых
--     строк — из ведомой до удаления, для вставляемых — из ведущей).
--
-- ДВА ИСТОЧНИКА, ОДНА ЛИНИЯ. doctype=6 собирается из PODCHECK (typehelp=4,
-- период с 2024-08) И из SPPODNORM (период до 2024-07), поэтому триггеры нужны
-- на ОБЕИХ таблицах и оба пишут tablename='PODCHECK'. Нумерация PODCHECK.ID и
-- SPPODNORM.IDRW независима, так что один idrw может отвечать за несколько
-- строк mocheck — delete_insert это штатно обрабатывает: удаляет по idrw все
-- строки в пределах doctype IN (5,6) и заливает всё, что ведущая отдаёт по
-- этому idrw сейчас.

-- id берётся из PODCHECK.ID (в MOCHECK.sql именно он подставлен в колонку
-- idrw), период — из того же выражения, что стоит в запросе за псевдонимом
-- createdate: TO_DATE(YEAR||'-01-01','YYYY-MM-DD'). Колонок IDRW и CREATEDATE
-- в PODCHECK нет.
CREATE OR REPLACE TRIGGER tr_podcheck_after_iud
AFTER INSERT OR UPDATE OR DELETE ON podcheck
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.id); p_oper := 'IU';
        p_period := TO_DATE(:new.year || '-01-01', 'YYYY-MM-DD');
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.id); p_oper := 'IU';
        p_period := TO_DATE(:new.year || '-01-01', 'YYYY-MM-DD');
        -- Сменился ключ — старую строку тоже надо переработать.
        IF :old.id <> :new.id THEN
            INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('PODCHECK', systimestamp, 'D',
                    TO_DATE(:old.year || '-01-01', 'YYYY-MM-DD'),
                    TO_CHAR(:old.id), 0);
        END IF;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.id); p_oper := 'D';
        p_period := TO_DATE(:old.year || '-01-01', 'YYYY-MM-DD');
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('PODCHECK', systimestamp, p_oper, p_period, p_id, 0);
END;
/

-- 5. TFINSCHET (линия TFINSCHET, mocheck doctype=9, режим iud).
--    Тот же случай «псевдоним запроса != колонка ведущей»: период линии
--    называется createdate, а в TFINSCHET это dschet (см. MOCHECK.sql:
--    "... dschet createdate ... FROM TFINSCHET").
CREATE OR REPLACE TRIGGER tr_tfinschet_after_iud
AFTER INSERT OR UPDATE OR DELETE ON tfinschet
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.dschet;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.dschet;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.dschet;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('TFINSCHET', systimestamp, p_oper, p_period, p_id, 0);
END;
/

CREATE OR REPLACE TRIGGER tr_sppodnorm_after_iud
AFTER INSERT OR UPDATE OR DELETE ON sppodnorm
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    -- В mocheck колонка idrw для этой ветки берётся из SPPODNORM.IDRW
    -- (см. MOCHECK.sql: "SELECT ... idrw id ... FROM SPPODNORM").
    -- Периода-даты у таблицы нет — собираем из year/month, он всё равно
    -- декоративный.
    IF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D';
        p_period := TO_DATE(:old.year || '-' || LPAD(:old.month, 2, '0') || '-01',
                            'YYYY-MM-DD');
    ELSE
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU';
        p_period := TO_DATE(:new.year || '-' || LPAD(:new.month, 2, '0') || '-01',
                            'YYYY-MM-DD');
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('PODCHECK', systimestamp, p_oper, p_period, p_id, 0);
END;
/

-- Снять прежние триггеры линий PODCHECK3 / PODCHECK4, если они были отдельными
-- (имена подставить свои — в репозитории их текста нет):
--   DROP TRIGGER tr_podcheck3_after_iud;
--   DROP TRIGGER tr_podcheck4_after_iud;

--------------------------------------------------------------------------------
-- ПЕРЕЕЗД PODCHECK3/PODCHECK4 -> PODCHECK: что сделать в БД один раз.
--
-- Выполнять ПОСЛЕ выкладки нового конфига и триггеров. Порядок важен: пока в
-- журнале лежат строки со старыми tablename, их не видит ни старая линия (её
-- конфига уже нет), ни новая (она читает только 'PODCHECK').
--------------------------------------------------------------------------------

-- 1. Необработанные события журнала перевести на новую линию, иначе изменения,
--    накопившиеся с последнего прогона, потеряются.
--    UPDATE etl_log_iud_row SET tablename = 'PODCHECK'
--    WHERE tablename IN ('PODCHECK3', 'PODCHECK4') AND isetl = 0;
--    COMMIT;
--
--    Строки с isetl = -1 — это записи, припаркованные прошлыми падениями. Их
--    стоит перенести ТОЖЕ и сбросить в 0, чтобы новая линия их доработала:
--    UPDATE etl_log_iud_row SET tablename = 'PODCHECK', isetl = 0
--    WHERE tablename IN ('PODCHECK3', 'PODCHECK4') AND isetl = -1;
--    COMMIT;

-- 2. Группы etl_jobs. У старых линий свои строки (tablename='PODCHECK3'/'4') с
--    теми же периодами — просто переименовать нельзя, получишь дубли по
--    (tablename, period). Заводим группы новой линии, старые убираем:
--    INSERT INTO etl_jobs (tablename, period, last_success_ts, isokaudit)
--    SELECT DISTINCT 'PODCHECK', period, NULL, 4
--    FROM   etl_jobs j
--    WHERE  j.tablename IN ('PODCHECK3', 'PODCHECK4')
--    AND    NOT EXISTS (SELECT 1 FROM etl_jobs e
--                       WHERE e.tablename = 'PODCHECK' AND e.period = j.period);
--    DELETE FROM etl_jobs WHERE tablename IN ('PODCHECK3', 'PODCHECK4');
--    COMMIT;
--
--    isokaudit = 4 выставлен намеренно: он заставит один раз перелить все
--    периоды целиком и тем самым вычистить осиротевшие строки, накопленные
--    раздельными линиями. Не нужна разовая перезаливка — ставь 0.

-- 3. Проверка: в журнале и etl_jobs не должно остаться старых имён.
--    SELECT tablename, isetl, COUNT(*) FROM etl_log_iud_row
--    WHERE tablename LIKE 'PODCHECK%' GROUP BY tablename, isetl;
--    SELECT tablename, isokaudit, COUNT(*) FROM etl_jobs
--    WHERE tablename LIKE 'PODCHECK%' GROUP BY tablename, isokaudit;
