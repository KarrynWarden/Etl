-- Триггер IUD для линий EXPMED23, EXPMED4 (Orcl).
-- Ведущая: EXPMED; журнал: koknaev.etl_log_iud_row.
-- Одна ведущая — несколько линий: EXPMED23 (период docexpdt), EXPMED4 (период docpenaltydt).
-- Триггер пишет в журнал по строке НА КАЖДУЮ линию, одинаково и
-- безусловно: разбирать, какая строка к какой линии относится, он
-- не должен и не умеет. Лишняя запись стоит одной проверки среза
-- у чужой линии и ничего не портит; отсутствующая стоила бы
-- потерянного изменения.
-- Сгенерирован конструктором (tools/trigger_builder.py); правки
-- руками при пересборке линии будут перезаписаны.
CREATE OR REPLACE TRIGGER tr_expmed_after_iud
AFTER INSERT OR UPDATE OR DELETE ON EXPMED
FOR EACH ROW
DECLARE
    p_id      VARCHAR2(200);
    p_oper    VARCHAR2(2);
    p_period1 DATE;
    p_period2 DATE;
BEGIN
    IF INSERTING THEN
        p_id      := TO_CHAR(:new.idrw);
        p_oper    := 'IU';
        p_period1 := :new.docexpdt;
        p_period2 := :new.docpenaltydt;
    ELSIF UPDATING THEN
        p_id      := TO_CHAR(:new.idrw);
        p_oper    := 'IU';
        p_period1 := :new.docexpdt;
        p_period2 := :new.docpenaltydt;
        -- EXPMED23: сменился PK или период — старую строку ведомой нужно удалить.
        IF TO_CHAR(:old.idrw) <> TO_CHAR(:new.idrw)
           OR NVL(:old.docexpdt, TO_DATE('1900-01-01', 'YYYY-MM-DD')) <> NVL(:new.docexpdt, TO_DATE('1900-01-01', 'YYYY-MM-DD')) THEN
            INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('EXPMED23', systimestamp, 'D', :old.docexpdt, TO_CHAR(:old.idrw), 0);
        END IF;
        -- EXPMED4: сменился PK или период — старую строку ведомой нужно удалить.
        IF TO_CHAR(:old.idrw) <> TO_CHAR(:new.idrw)
           OR NVL(:old.docpenaltydt, TO_DATE('1900-01-01', 'YYYY-MM-DD')) <> NVL(:new.docpenaltydt, TO_DATE('1900-01-01', 'YYYY-MM-DD')) THEN
            INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('EXPMED4', systimestamp, 'D', :old.docpenaltydt, TO_CHAR(:old.idrw), 0);
        END IF;
    ELSIF DELETING THEN
        p_id      := TO_CHAR(:old.idrw);
        p_oper    := 'D';
        p_period1 := :old.docexpdt;
        p_period2 := :old.docpenaltydt;
    END IF;

    INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('EXPMED23', systimestamp, p_oper, p_period1, p_id, 0);
    INSERT INTO koknaev.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('EXPMED4', systimestamp, p_oper, p_period2, p_id, 0);
END;
/
