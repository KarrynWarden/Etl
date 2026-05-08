--------------------------------------------------------------------------------
-- Шаблон триггера IUD для произвольной ведущей таблицы Oracle.
--
-- Подставить:
--   <TRIGGER_NAME>     — имя триггера (обычно tr_<TABLE>_after_iud)
--   <TABLE_NAME>       — имя ведущей таблицы (как в etl_jobs.tablename, в нижнем регистре)
--   <PERIOD_COLUMN>    — имя колонки группировки (createdate или эквивалент)
--   <PK_EXPR_NEW>      — выражение PK по :new (для составного — конкатенация
--                        через '/', например :new.idrw1 || '/' || :new.idrw2)
--   <PK_EXPR_OLD>      — то же по :old
--
-- Если ведущая участвует одновременно в нескольких ETL-направлениях
-- (например, reqprepmo идёт и в orcl, и в post-post), нужно создать
-- отдельные триггеры с разными <TABLE_NAME> (равными tableNameEtlJobs).
--------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER <TRIGGER_NAME>
AFTER INSERT OR UPDATE OR DELETE ON <TABLE_NAME>
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id     := <PK_EXPR_NEW>;
        p_oper   := 'IU';
        p_period := :new.<PERIOD_COLUMN>;
    ELSIF UPDATING THEN
        p_id     := <PK_EXPR_NEW>;
        p_oper   := 'IU';
        p_period := :new.<PERIOD_COLUMN>;
        -- Если поменялся PK или период, старую запись тоже нужно удалить.
        IF <PK_EXPR_OLD> <> <PK_EXPR_NEW>
           OR NVL(:old.<PERIOD_COLUMN>, TO_DATE('1900-01-01','YYYY-MM-DD'))
            <> NVL(:new.<PERIOD_COLUMN>, TO_DATE('1900-01-01','YYYY-MM-DD')) THEN
            INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, iseth)
            VALUES ('<TABLE_NAME>', systimestamp, 'D',
                    :old.<PERIOD_COLUMN>, <PK_EXPR_OLD>, 0);
        END IF;
    ELSIF DELETING THEN
        p_id     := <PK_EXPR_OLD>;
        p_oper   := 'D';
        p_period := :old.<PERIOD_COLUMN>;
    END IF;

    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, iseth)
    VALUES ('<TABLE_NAME>', systimestamp, p_oper, p_period, p_id, 0);
END;
/
