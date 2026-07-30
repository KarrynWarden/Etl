--------------------------------------------------------------------------------
-- EindexMO: подготовка ведущей таблицы на PostgreSQL к переносу PG → Oracle
-- (тип линии — «ETL-табл», журнал изменений etl_user.etl_log_iud_row).
--
-- Что делает скрипт (пункты ТЗ):
--   1.1  создаёт поле public.eindexmo.createdate типа date (без времени);
--   1.2  заполняет createdate значениями lastupdate::date (timestamp → date);
--   1.4  BEFORE INSERT триггер, который проставляет createdate новым записям;
--   1.5  AFTER INSERT/UPDATE — запись 'IU' в etl_user.etl_log_iud_row;
--   1.6  AFTER DELETE      — запись 'D'  в etl_user.etl_log_iud_row.
--
-- Пункты 1.1 (Oracle) и 1.3 (снятие триггеров на Oracle) — в
-- etlFolder/queries/oracleSetup/07_eindexmo_prepare.sql.
--
-- 1.5 и 1.6 реализованы ОДНИМ AFTER INSERT OR UPDATE OR DELETE триггером —
-- так же, как у prbdir/reqprepmo (см. 03_example_triggers.sql). Оба INSERT'а
-- из ТЗ в нём есть, различаются только oper ('IU' / 'D').
--
-- Перед запуском проверить:
--   * реальные имена схемы/таблицы (ниже везде public.eindexmo);
--   * что PK ведущей — idrw (иначе поправить pId и условие смены ключа);
--   * что журнал etl_user.etl_log_iud_row существует и в него есть INSERT.
--------------------------------------------------------------------------------

-- 0. Проверки перед стартом ----------------------------------------------------
-- Структура ведущей (ожидаем idrw и lastupdate timestamp):
--   select column_name, data_type
--     from information_schema.columns
--    where table_schema = 'public' and table_name = 'eindexmo'
--    order by ordinal_position;
-- Первичный ключ:
--   select a.attname
--     from pg_index i
--     join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
--    where i.indrelid = 'public.eindexmo'::regclass and i.indisprimary;
-- Журнал:
--   select count(*) from etl_user.etl_log_iud_row where tablename = 'eindexmo';


-- 1.1. Поле createdate (дата без времени) --------------------------------------
ALTER TABLE public.eindexmo
    ADD COLUMN IF NOT EXISTS createdate date;


-- 1.2. Перенос значений lastupdate → createdate --------------------------------
-- timestamp → date: приведение ::date отбрасывает время, часовой пояс не
-- участвует (lastupdate — timestamp without time zone).
--
-- ВНИМАНИЕ: на большой таблице это один длинный UPDATE с полным переписыванием
-- строк. Если таблица объёмная — выполнять порциями (пример в конце файла),
-- иначе распухнет WAL и надолго встанут блокировки.
UPDATE public.eindexmo
   SET createdate = lastupdate::date
 WHERE createdate IS DISTINCT FROM lastupdate::date;

-- У строк с пустым lastupdate createdate останется NULL, а ETL и аудит ходят
-- по периодам — такие записи не попадут ни в одну группу. Проверить и решить,
-- чем их закрывать (например, датой заведения линии):
--   select count(*) from public.eindexmo where createdate is null;

-- Индекс под выборку периодов ETL (DateSelectMasterPost / DeletePeriodPost
-- ходят по createdate) и под аудит.
CREATE INDEX IF NOT EXISTS ix_eindexmo_createdate
    ON public.eindexmo (createdate);

ANALYZE public.eindexmo;


-- 1.4. Заполнение createdate при вставке ---------------------------------------
-- Отдельный BEFORE INSERT триггер: менять NEW можно только в BEFORE-триггере,
-- в AFTER-триггере присваивание уже ни на что не влияет.
-- Если createdate передан явно — он сохраняется; иначе берётся дата из
-- lastupdate (так же, как при первичном заполнении в п. 1.2), а при пустом
-- lastupdate — текущая дата.
CREATE OR REPLACE FUNCTION public.tr_etl_eindexmo_before_i_func()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
 begin
  if new.createdate is null then
   new.createdate := coalesce(new.lastupdate::date, current_date);
  end if;
  return new;
 END;
$function$
;

DROP TRIGGER IF EXISTS tr_etl_eindexmo_before_i ON public.eindexmo;

CREATE TRIGGER tr_etl_eindexmo_before_i BEFORE
insert
    on
    public.eindexmo for each row execute function tr_etl_eindexmo_before_i_func();


-- 1.5 + 1.6. Логирование записей, подлежащих ETL --------------------------------
-- SECURITY DEFINER — чтобы прикладным пользователям, пишущим в eindexmo, не
-- требовались права на etl_user.etl_log_iud_row (так же сделано у reqprepmo).
CREATE OR REPLACE FUNCTION public.tr_etl_eindexmo_after_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = pg_catalog, public
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

DROP TRIGGER IF EXISTS tr_etl_eindexmo_after_iud ON public.eindexmo;

CREATE TRIGGER tr_etl_eindexmo_after_iud AFTER
insert
    or
delete
    or
update
    on
    public.eindexmo for each row execute function tr_etl_eindexmo_after_iud_func();


-- Проверка ---------------------------------------------------------------------
-- Триггеры на месте:
--   select tgname, tgenabled, pg_get_triggerdef(oid)
--     from pg_trigger
--    where tgrelid = 'public.eindexmo'::regclass and not tgisinternal;
--
-- Дымовой тест (в транзакции с откатом, idrw подставить свой):
--   begin;
--     insert into public.eindexmo (idrw, lastupdate) values (-1, clock_timestamp());
--     update public.eindexmo set lastupdate = clock_timestamp() where idrw = -1;
--     delete from public.eindexmo where idrw = -1;
--     select oper, period, id, isetl from etl_user.etl_log_iud_row
--      where tablename = 'eindexmo' and id = '-1' order by idrw;
--     -- ожидаем: IU (createdate = сегодня), IU, D
--   rollback;


-- Порционное заполнение createdate (альтернатива п. 1.2 на большой таблице) ----
-- Выполнять ВНЕ транзакции (в psql/DBeaver как отдельный оператор): каждая
-- порция коммитится своей процедурой.
--
-- do $$
-- declare
--  n integer;
-- begin
--  loop
--   update public.eindexmo e
--      set createdate = e.lastupdate::date
--    where e.ctid in (
--          select ctid from public.eindexmo
--           where createdate is null and lastupdate is not null
--           limit 50000);
--   get diagnostics n = row_count;
--   exit when n = 0;
--   commit;
--   raise notice 'обновлено %', n;
--  end loop;
-- end $$;


-- Откат ------------------------------------------------------------------------
-- drop trigger if exists tr_etl_eindexmo_after_iud on public.eindexmo;
-- drop trigger if exists tr_etl_eindexmo_before_i on public.eindexmo;
-- drop function if exists public.tr_etl_eindexmo_after_iud_func();
-- drop function if exists public.tr_etl_eindexmo_before_i_func();
-- drop index if exists public.ix_eindexmo_createdate;
-- alter table public.eindexmo drop column if exists createdate;
