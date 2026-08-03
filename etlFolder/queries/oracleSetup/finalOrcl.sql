CREATE SEQUENCE etl_log_iud_row_seq START WITH 1 INCREMENT BY 1 NOCACHE;

-- 2. Таблица.
CREATE TABLE etl_log_iud_row (
    idrw      NUMBER        PRIMARY KEY,
    tablename VARCHAR2(100) NOT NULL,
    timeoper  TIMESTAMP     DEFAULT systimestamp,
    oper      VARCHAR2(2)   NOT NULL,
    period    DATE,
    id        VARCHAR2(200) NOT NULL,
    isetl     NUMBER(1)     DEFAULT 0
);

-- 3. Автозаполнение idrw из последовательности (совместимо со старыми
--    версиями Oracle без identity/DEFAULT seq.NEXTVAL).
CREATE OR REPLACE TRIGGER trg_etl_log_iud_row_bi
BEFORE INSERT ON etl_log_iud_row
FOR EACH ROW
BEGIN
    IF :NEW.idrw IS NULL THEN
        SELECT etl_log_iud_row_seq.NEXTVAL INTO :NEW.idrw FROM DUAL;
    END IF;
END;


-- 4. Индексы под выборку ETL-процессом.
CREATE INDEX ix_etl_log_iud_row_tabl_isetl ON etl_log_iud_row(tablename, isetl);
CREATE INDEX ix_etl_log_iud_row_per ON etl_log_iud_row(period, id);

-- 5. Доступ ETL-пользователю (если журнал читается из-под отдельной учётки).
GRANT SELECT, INSERT, UPDATE, DELETE ON etl_log_iud_row TO etl_user;


CREATE OR REPLACE TRIGGER KOKNAEV.tr_planoms_after_iud
AFTER INSERT OR UPDATE OR DELETE ON planoms
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.createdate;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('PLANOMS', systimestamp, p_oper, p_period, p_id, 0);
END;

CREATE OR REPLACE TRIGGER tr_planomsdet_after_iud
AFTER INSERT OR UPDATE OR DELETE ON planomsdet
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.createdate;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('PLANOMSDET', systimestamp, p_oper, p_period, p_id, 0);
END;

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
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.updt;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('MEDCHECK', systimestamp, p_oper, p_period, p_id, 0);
END;

CREATE OR REPLACE TRIGGER tr_expmed_after_iud
AFTER INSERT OR UPDATE OR DELETE ON expmed
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.docexpdt;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := :new.docexpdt;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := :old.docexpdt;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('EXPMED', systimestamp, p_oper, p_period, p_id, 0);
END;

CREATE OR REPLACE TRIGGER tr_podcheck_after_iud
AFTER INSERT OR UPDATE OR DELETE ON podcheck
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := TO_DATE(:new.year||'-01-01','YYYY-MM-DD');
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idrw); p_oper := 'IU'; p_period := TO_DATE(:new.year||'-01-01','YYYY-MM-DD');
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idrw); p_oper := 'D'; p_period := TO_DATE(:old.year||'-01-01','YYYY-MM-DD');
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('PODCHECK', systimestamp, p_oper, p_period, p_id, 0);
END;


grant select on etl_log_iud_row to bashkirova

grant select on etl_jobs to bashkirova

grant select on etl_log to bashkirova

CREATE OR REPLACE TRIGGER KOKNAEV.tr_tfinschet_after_iud
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

CREATE OR REPLACE TRIGGER KOKNAEV.tr_tfoutschet_after_iud
AFTER INSERT OR UPDATE OR DELETE ON tfoutschet
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
    VALUES ('TFOUTSCHET', systimestamp, p_oper, p_period, p_id, 0);
END;


ALTER TABLE koknaev.eindexmo ADD (createdate DATE);

UPDATE koknaev.eindexmo
   SET createdate = TRUNC(lastupdate)
 WHERE createdate != TRUNC(lastupdate)
    OR createdate IS NULL;

CREATE INDEX koknaev.ix_eindexmo_createdate ON koknaev.eindexmo (createdate);


CREATE OR REPLACE TRIGGER "KOKNAEV"."LOGEINDPROLONG_INSERT_TRIGGER" 
  before insert on koknaev.LOG_EINDPROLONG
  FOR EACH ROW
DECLARE
  -- local variables here
BEGIN
  :new.lastupdate:=sysdate;
  :new.createdate:=trunc(sysdate);
  SELECT log_eindprolong_incr.nextval INTO :new.idlog FROM dual;

END LOGEINDPROLONG_INSERT_TRIGGER;

grant select, insert, update, delete on EINDEXSTRU to etl_user

grant select, insert, update, delete on SPEINDEXFORM to etl_user

grant select, insert, update, delete on SPEINDEX to etl_user

grant select, insert, update, delete on SPFCOST to etl_user

grant select, insert, update, delete on EINDEXMO to etl_user

CREATE OR REPLACE TRIGGER KOKNAEV.tr_log_eindprolong_after_iud
AFTER INSERT OR UPDATE OR DELETE ON log_eindprolong
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id := TO_CHAR(:new.idlog); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF UPDATING THEN
        p_id := TO_CHAR(:new.idlog); p_oper := 'IU'; p_period := :new.createdate;
    ELSIF DELETING THEN
        p_id := TO_CHAR(:old.idlog); p_oper := 'D'; p_period := :old.createdate;
    END IF;
    INSERT INTO etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('LOG_EINDPROLONG', systimestamp, p_oper, p_period, p_id, 0);
END; 