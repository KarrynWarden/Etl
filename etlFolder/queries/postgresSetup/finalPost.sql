CREATE UNIQUE INDEX idx_mochek_doctype1_unique 
ON public.mocheck (doctype, idrw) 
WHERE doctype = 1;

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

INSERT INTO ETL_JOBS (TABLENAME, PERIOD, LAST_SUCCESS_TS, ISOKAUDIT) VALUES ('SPEINDEX', current_date, current_date, 0)

INSERT INTO ETL_JOBS (TABLENAME, PERIOD, LAST_SUCCESS_TS, ISOKAUDIT) VALUES ('SPEINDEXFORM', current_date, current_date, 0)

INSERT INTO ETL_JOBS (TABLENAME, PERIOD, LAST_SUCCESS_TS, ISOKAUDIT) VALUES ('EINDEXSTRU', current_date, current_date, 0)

INSERT INTO ETL_JOBS (TABLENAME, PERIOD, LAST_SUCCESS_TS, ISOKAUDIT) VALUES ('SPFCOST', current_date, current_date, 0)


create trigger tr_speindex_iud before
insert
    or
delete
    or
update
    on
    public.speindex for each row execute function tr_etl_table_iud_func('SPEINDEX')

create trigger tr_speindexform_iud before
insert
    or
delete
    or
update
    on
    public.speindexform for each row execute function tr_etl_table_iud_func('SPEINDEXFORM')

create trigger tr_eindexstru_iud before
insert
    or
delete
    or
update
    on
    public.eindexstru for each row execute function tr_etl_table_iud_func('EINDEXSTRU')

create trigger tr_spfcost_iud before
insert
    or
delete
    or
update
    on
    public.spfcost for each row execute function tr_etl_table_iud_func('SPFCOST')


ALTER TABLE public.eindexmo
    ADD COLUMN IF NOT EXISTS createdate date;

--CREATE INDEX IF NOT EXISTS ix_eindexmo_createdate
--    ON public.eindexmo (createdate);

--    ANALYZE public.eindexmo;


-- DROP FUNCTION public.tr_etl_eindexmo_after_iud_func();

CREATE OR REPLACE FUNCTION public.tr_etl_eindexmo_after_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
 declare
  pId numeric;
  pOper varchar;
  pPeriod date;
  pTableName varchar;
 begin
  pTableName := 'eindexmo';

  if TG_OP = 'DELETE' then
   pId := old.idrw;
   pOper := 'D';
   pPeriod := old.createdate;
  else
   pId := new.idrw;
   pOper := 'IU';
   pPeriod := new.createdate;

   --- если меняется ключ или период, то старую запись помечаем для удаления:
   --- ведомая ищется по паре (period, idrw), и без этого на Oracle останется
   --- строка-сирота в прежнем периоде
   if TG_OP = 'UPDATE'
      and (old.idrw is distinct from new.idrw
           or old.createdate is distinct from new.createdate) then
    insert into etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    values (pTableName, clock_timestamp(), 'D', old.createdate, old.idrw, 0);
   end if;
  end if;

  insert into etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
  values (pTableName, clock_timestamp(), pOper, pPeriod, pId, 0);

  --- AFTER-триггер: возвращаемое значение игнорируется
  return null;
 END;
$function$
;


create trigger tr_etl_eindexmo_after_iud after
insert
    or
delete
    or
update
    on
    public.eindexmo for each row execute function tr_etl_eindexmo_after_iud_func()

CREATE OR REPLACE FUNCTION public.tr_prbdir_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
 declare
  pId numeric;
  pOper varchar;
  pPeriod date;
  pTableName varchar;
 begin
  pTableName := 'prbdir';

  if TG_OP = 'DELETE' then
   pId := old.idrw;
   pOper := 'D';
   pPeriod := old.createdate;
  else
   pId := new.idrw;
   pOper := 'IU';
   pPeriod := new.createdate;

   --- если меняется ключ или период, то старую запись помечаем для удаления:
   --- ведомая ищется по паре (period, idrw), и без этого на Oracle останется
   --- строка-сирота в прежнем периоде
   if TG_OP = 'UPDATE'
      and (old.idrw is distinct from new.idrw
           or old.createdate is distinct from new.createdate) then
    insert into etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    values (pTableName, clock_timestamp(), 'D', old.createdate, old.idrw, 0);
   end if;
  end if;

  insert into etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
  values (pTableName, clock_timestamp(), pOper, pPeriod, pId, 0);

  --- AFTER-триггер: возвращаемое значение игнорируется
  return null;
 END;
$function$
;


CREATE OR REPLACE FUNCTION public.tr_etl_eindexmo_before_i_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Если хотите всегда перезаписывать createdate текущей датой:
        -- NEW.createdate := CURRENT_DATE;

        -- Если хотите сохранять переданное значение, а дату ставить только если NULL:
        NEW.createdate := COALESCE(NEW.createdate, CURRENT_DATE);

        NEW.lastupdate := clock_timestamp();
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN


        NEW.lastupdate := clock_timestamp();
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$function$;


create trigger tr_etl_eindexmo_before_i before
insert
    or
update
    on
    public.eindexmo for each row execute function tr_etl_eindexmo_before_i_func()

DROP TRIGGER IF EXISTS tr_pddir_iud ON public.pddir;

CREATE OR REPLACE FUNCTION public.tr_pddir_set_ts_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.createdate := COALESCE(NEW.createdate, CURRENT_DATE);
        NEW.lastupdate := clock_timestamp();
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        NEW.createdate := COALESCE(NEW.createdate, OLD.createdate);
        NEW.lastupdate := clock_timestamp();
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$function$;

CREATE TRIGGER tr_pddir_set_ts
BEFORE INSERT OR UPDATE
ON public.pddir
FOR EACH ROW
EXECUTE FUNCTION public.tr_pddir_set_ts_func();


CREATE OR REPLACE FUNCTION public.tr_pddir_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
DECLARE
    pTableName text := 'pddir';
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
        VALUES (pTableName, clock_timestamp(), 'D', OLD.createdate, OLD.idrw, 0);

    ELSE
        IF TG_OP = 'UPDATE'
           AND (OLD.idrw IS DISTINCT FROM NEW.idrw
                OR OLD.createdate IS DISTINCT FROM NEW.createdate) THEN
            INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES (pTableName, clock_timestamp(), 'D', OLD.createdate, OLD.idrw, 0);
        END IF;

        INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
        VALUES (pTableName, clock_timestamp(), 'IU', NEW.createdate, NEW.idrw, 0);
    END IF;

    RETURN NULL;
END;
$function$;

CREATE TRIGGER tr_pddir_iud
AFTER INSERT OR UPDATE OR DELETE
ON public.pddir
FOR EACH ROW
EXECUTE FUNCTION public.tr_pddir_iud_func();