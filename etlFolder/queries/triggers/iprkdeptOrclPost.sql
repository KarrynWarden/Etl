-- Триггер IUD для линии iprkdept (Orcl).
-- Ведущая: KOKNAEV.IPRKDEPT; журнал: koknaev.etl_log_iud_row.
-- Сгенерирован конструктором (tools/trigger_builder.py); правки
-- руками при пересборке линии будут перезаписаны.
CREATE OR REPLACE TRIGGER tr_iprkdept_after_iud
AFTER INSERT OR UPDATE OR DELETE ON KOKNAEV.IPRKDEPT
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id     := TO_CHAR(:new.IDRW);
        p_oper   := 'IU';
        p_period := :new.createdate;
    ELSIF UPDATING THEN
        p_id     := TO_CHAR(:new.IDRW);
        p_oper   := 'IU';
        p_period := :new.createdate;
        -- Сменился PK или период — старую строку ведомой нужно удалить.
        IF TO_CHAR(:old.IDRW) <> TO_CHAR(:new.IDRW)
           OR NVL(:old.createdate, TO_DATE('1900-01-01', 'YYYY-MM-DD')) <> NVL(:new.createdate, TO_DATE('1900-01-01', 'YYYY-MM-DD')) THEN
            INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('iprkdept', systimestamp, 'D', :old.createdate, TO_CHAR(:old.IDRW), 0);
        END IF;
    ELSIF DELETING THEN
        p_id     := TO_CHAR(:old.IDRW);
        p_oper   := 'D';
        p_period := :old.createdate;
    END IF;

    INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('iprkdept', systimestamp, p_oper, p_period, p_id, 0);
END;
/
