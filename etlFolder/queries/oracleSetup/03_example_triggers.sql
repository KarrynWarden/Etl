--------------------------------------------------------------------------------
-- Примеры готовых триггеров на основе шаблона 02_trigger_template.sql.
--
-- 1) Простая таблица medcheck (поле периода — UPDT, PK — IDRW).
-- 2) reqprepmo, которая участвует одновременно в двух направлениях
--    (orcl<->post и post<->post в роли mocheck) — два отдельных триггера
--    с разными tablename.
-- 3) mocheck-style: одна таблица, разделяемая по doctype, требует только
--    один триггер; разделение по группам берёт на себя селект-источник.
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
