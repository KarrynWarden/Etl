-- Триггер IUD для линии EXPMED (Orcl).
-- Ведущая: EXPMED; журнал: koknaev.etl_log_iud_row.
-- Сгенерирован конструктором (tools/trigger_builder.py); правки
-- руками при пересборке линии будут перезаписаны.
CREATE OR REPLACE TRIGGER tr_expmed_after_iud
AFTER INSERT OR UPDATE OR DELETE ON EXPMED
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id     := TO_CHAR(:new.idrw);
        p_oper   := 'IU';
        p_period := :new.docexpdt;
    ELSIF UPDATING THEN
        p_id     := TO_CHAR(:new.idrw);
        p_oper   := 'IU';
        p_period := :new.docexpdt;
        -- Сменился PK или период — старую строку ведомой нужно удалить.
        IF TO_CHAR(:old.idrw) <> TO_CHAR(:new.idrw)
           OR NVL(:old.docexpdt, TO_DATE('1900-01-01', 'YYYY-MM-DD')) <> NVL(:new.docexpdt, TO_DATE('1900-01-01', 'YYYY-MM-DD')) THEN
            INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('EXPMED', systimestamp, 'D', :old.docexpdt, TO_CHAR(:old.idrw), 0);
        END IF;
    ELSIF DELETING THEN
        p_id     := TO_CHAR(:old.idrw);
        p_oper   := 'D';
        p_period := :old.docexpdt;
    END IF;

    INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('EXPMED', systimestamp, p_oper, p_period, p_id, 0);
END;
/
