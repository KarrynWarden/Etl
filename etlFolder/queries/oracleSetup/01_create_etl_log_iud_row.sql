--------------------------------------------------------------------------------
-- Создание таблицы etl_log_iud_row на Oracle.
--
-- Назначение: журнал INSERT/UPDATE/DELETE на ведущих таблицах. ETL-процесс
-- читает из этой таблицы строки с isetl = 0 и переносит соответствующие
-- записи в ведомую БД.
--
-- Поля совместимы с PostgreSQL-аналогом из etl_user.etl_log_iud_row.
--   tablename : имя группы (как в etl_jobs.tablename)
--   timeoper  : момент операции
--   oper      : 'IU' (insert+update объединены через MERGE) или 'D'
--   period    : значение createdate (или эквивалентного поля группировки)
--   id        : первичный ключ изменённой записи; для составного PK
--                сохраняется как 'pk1/pk2/...'
--   isetl     : 0 — требует обработки, 1 — обработано, -1 — ошибка ETL
--   idrw      : суррогатный ключ
--------------------------------------------------------------------------------

CREATE SEQUENCE etl_log_iud_row_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE etl_log_iud_row (
    idrw      NUMBER       DEFAULT etl_log_iud_row_seq.NEXTVAL PRIMARY KEY,
    tablename VARCHAR2(100) NOT NULL,
    timeoper  TIMESTAMP    DEFAULT systimestamp,
    oper      VARCHAR2(2)  NOT NULL,
    period    DATE,
    id        VARCHAR2(200) NOT NULL,
    isetl     NUMBER(1)    DEFAULT 0
);

CREATE INDEX ix_etl_log_iud_row_tabl_isetl
    ON etl_log_iud_row(tablename, isetl);

CREATE INDEX ix_etl_log_iud_row_per
    ON etl_log_iud_row(period, id);


CREATE TABLE etl_log_iud_row (
    idrw      NUMBER       PRIMARY KEY, -- Убрали DEFAULT
    tablename VARCHAR2(100) NOT NULL,
    timeoper  TIMESTAMP    DEFAULT systimestamp,
    oper      VARCHAR2(2)  NOT NULL,
    period    DATE,
    id        VARCHAR2(200) NOT NULL,
    isetl     NUMBER(1)    DEFAULT 0
);

-- 3. Создаем триггер для автозаполнения ID
CREATE OR REPLACE TRIGGER trg_etl_log_iud_row_bi
BEFORE INSERT ON etl_log_iud_row
FOR EACH ROW
BEGIN
    IF :NEW.idrw IS NULL THEN
        SELECT etl_log_iud_row_seq.NEXTVAL INTO :NEW.idrw FROM DUAL;
    END IF;
END;

-- 4. Создаем индексы
CREATE INDEX ix_etl_log_iud_row_tabl_isetl ON etl_log_iud_row(tablename, isetl);
CREATE INDEX ix_etl_log_iud_row_per ON etl_log_iud_row(period, id);

grant select, insert, update, delete on etl_log_iud_row to etl_user








--ниже новый скрипт, но он ещё непроверен
--------------------------------------------------------------------------------
-- Создание таблицы etl_log_iud_row на Oracle.
--
-- Назначение: журнал INSERT/UPDATE/DELETE на ведущих таблицах. ETL-процесс
-- читает из этой таблицы строки с isetl = 0 и переносит соответствующие
-- записи в ведомую БД.
--
-- Поля:
--   idrw      : суррогатный ключ (PRIMARY KEY), заполняется из
--               etl_log_iud_row_seq BEFORE-INSERT триггером ниже;
--   tablename : имя группы (как в etl_jobs.tablename);
--   timeoper  : момент операции;
--   oper      : 'IU' (insert+update объединены через MERGE) или 'D';
--   period    : значение createdate (или эквивалентного поля группировки).
--               ВНИМАНИЕ: для режима delete_insert (expmed) это поле
--               ДЕКОРАТИВНОЕ — реальный период ETL берёт из данных, см. README;
--   id        : первичный ключ изменённой записи; для составного PK
--               сохраняется как 'pk1/pk2/...';
--   isetl     : 0 — требует обработки, 1 — обработано, -1 — ошибка ETL.
--
-- ВАЖНО (ORA-04098). Последовательность и триггер должны жить в ОДНОЙ схеме
-- с таблицей. Если триггер trg_etl_log_iud_row_bi не видит
-- etl_log_iud_row_seq (последовательности нет / она в другой схеме), он
-- становится INVALID и любой INSERT в etl_log_iud_row падает с ORA-04098
-- (а вслед за ним — триггеры ведущих таблиц, tr_<table>_after_iud).
-- Диагностика:
--   ALTER TRIGGER trg_etl_log_iud_row_bi COMPILE;
--   SELECT text FROM user_errors WHERE name='TRG_ETL_LOG_IUD_ROW_BI';
--------------------------------------------------------------------------------

-- 1. Последовательность для idrw.
--    Если таблица уже содержит данные — START WITH должен быть ВЫШЕ
--    текущего MAX(idrw): SELECT NVL(MAX(idrw),0)+1 FROM etl_log_iud_row;
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


CREATE OR REPLACE TRIGGER tr_planoms_after_iud
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

grant select, insert, update, delete on etl_log_iud_row to bashkirova

grant select, insert, update, delete on etl_jobs to bashkirova

grant select, insert, update, delete on etl_log to bashkirova

grant select, insert, update, delete on ETL_LOG_DEL_ROW to bashkirova



CREATE TABLE iworkfoms (
    dact DATE,
    enp VARCHAR(16),
    oip VARCHAR(25),
    vpolis VARCHAR(1),
    npolis VARCHAR(16),
    w NUMERIC(1, 0),
    dr DATE,
    age VARCHAR(12),
    smo VARCHAR(6),
    ogrnsmo VARCHAR(15),
    okatoins VARCHAR(5),
    c_oksm VARCHAR(3),
    work NUMERIC(1, 0),
    kateg NUMERIC(2, 0),
    okatowork VARCHAR(5),
    doctype1 VARCHAR(10),
    doctype2 VARCHAR(10),
    ern VARCHAR(12),
    snils VARCHAR(11),
    dtcancel DATE,
    status NUMERIC(1, 0),
    dcreate DATE,
    iduser VARCHAR(40),
    id NUMERIC(10, 0)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON iworkfoms TO ias5_a57ro;


CREATE UNIQUE INDEX idx_mochek_doctype1_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype = 1;

CREATE UNIQUE INDEX idx_mochek_expmed_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype in (2,3,4);

CREATE UNIQUE INDEX idx_mochek_doctype5_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype = 5;

CREATE UNIQUE INDEX idx_mochek_doctype7_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype = 7;

CREATE UNIQUE INDEX idx_mochek_doctype8_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype = 8;

CREATE UNIQUE INDEX idx_mochek_doctype9_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype = 9;

CREATE OR REPLACE FUNCTION public.tr_etl_reqprepmo_after_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
	declare 
		pId numeric;
		pOper varchar;
		pPeriod date;
		pTableName varchar;
		pTableName2 varchar;
	begin
		pTableName := 'reqprepmo';
		pTableName2 := 'reqprepmomocheck';
	
		pId := new.idrw;
		pOper := 'IU';	
		pPeriod := new.createdate;		
	
	  	if TG_OP = 'INSERT' then
			old.createdate := clock_timestamp();
	  	elsif TG_OP = 'UPDATE' then
			--- если меняется ключ, то старую запись помечаем для удаления
	  		if old.idrw<>new.idrw then
				insert into etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
				values (pTableName, clock_timestamp(), 'D', pPeriod, old.idrw, 0),
					   (pTableName2, clock_timestamp(), 'D', pPeriod, old.idrw, 0);	  		
	  		end if;
		elsif TG_OP = 'DELETE' then
			pId := old.idrw;
			pOper := 'D';
			pPeriod := old.createdate;
	  	end if;
		insert into etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
		values (pTableName, clock_timestamp(), pOper, pPeriod, pId, 0),
			   (pTableName2, clock_timestamp(), pOper, pPeriod, pId, 0);
	  return old;
	END;
$function$
;



-- 1. Создаем последовательность
CREATE SEQUENCE ilog_idlog_seq;

ALTER SEQUENCE ilog_idlog_seq OWNER TO filonov;

-- 2. Привязываем последовательность к колонке 
-- (благодаря этому она автоматически удалится, если вы удалите колонку или таблицу)
ALTER SEQUENCE ilog_idlog_seq OWNED BY ilog.idlog;

-- 3. Настраиваем автоинкремент (значение по умолчанию для новых INSERT'ов)
ALTER TABLE ilog ALTER COLUMN idlog SET DEFAULT nextval('ilog_idlog_seq');

-- 4. Синхронизируем счетчик последовательности с максимальным ID в таблице.
-- COALESCE добавлен для безопасности на случай, если таблица вдруг окажется пустой.
-- Третий аргумент (true) означает, что следующий nextval() вернет MAX + 1 (то есть 2309596).
SELECT setval('ilog_idlog_seq', 2309596, true);

-- 5. Выдаем права на использование последовательности нужному пользователю
GRANT USAGE, SELECT ON SEQUENCE ilog_idlog_seq TO ias5_a57ro;


-- ЕНП, дополненный нулём до 16 знаков (algo1, algo2, algo5, algo6)
CREATE INDEX IF NOT EXISTS idx_iperson_enp16
    ON iperson (lpad(trim(enp::text), 16, '0'));

-- Полис, дополненный нулём до 16 знаков (algo1, algo3)
CREATE INDEX IF NOT EXISTS idx_iperson_npolis16
    ON iperson (lpad(trim(npolis::text), 16, '0'));

-- СНИЛС, нормализованный (только цифры, слева нули до 11) (algo1..algo4, algo6)
CREATE INDEX IF NOT EXISTS idx_iperson_snils11
    ON iperson (lpad(regexp_replace(trim(coalesce(ss, '')), '\D', '', 'g'), 11, '0'));

-- Дата рождения (algo1..algo5)
CREATE INDEX IF NOT EXISTS idx_iperson_dr
    ON iperson (dr);

CREATE INDEX IF NOT EXISTS idx_iferzl_oip_oip
    ON iferzl_oip (oip)
    WHERE typerow = 1;

ANALYZE iferzl_oip;

-- Обновляем статистику, чтобы планировщик начал использовать индексы
ANALYZE iperson;

CREATE SEQUENCE iworkfoms_idrw_seq START WITH 1 INCREMENT BY 1;

ALTER SEQUENCE iworkfoms_idrw_seq OWNED BY iworkfoms.idrw;

UPDATE iworkfoms SET idrw = nextval('iworkfoms_idrw_seq');

GRANT USAGE, select on sequence iworkfoms_idrw_seq TO ias5_a57ro;

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE iferzl_oip TO ias5_a57ro;

GRANT USAGE, select on sequence logmz_idlog_seq TO ias5_a57ro;

