--------------------------------------------------------------------------------
-- Аудитные триггеры СПРАВОЧНИКОВ на стороне Postgres: etl_jobs.isokaudit = 0.
--
-- Зачем и как — см. oracleSetup/09_sp_audit_triggers.sql (логика одна на обе
-- БД: даг SpEtlNew берёт работу из etl_jobs, а ноль туда ставит триггер).
--
-- Отличие Postgres: функция ОДНА на все справочники, а метка приходит в неё
-- аргументом (TG_ARGV[0]) — поэтому в самой функции литерала нет, он виден
-- только в определении триггера. Проверка конструктора это учитывает.
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.tr_etl_table_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    param text := TG_ARGV[0];
BEGIN
    UPDATE etl_user.etl_jobs SET isokaudit = 0, last_success_ts = clock_timestamp()
     WHERE tablename = param;
    IF TG_OP = 'DELETE' THEN
        RETURN old;
    END IF;
    RETURN new;
END;
$function$;

-- Пример: справочник EINDEXSTRU (ведущая public.eindexstru).
DROP TRIGGER IF EXISTS tr_eindexstru_iud ON public.eindexstru;
CREATE TRIGGER tr_eindexstru_iud
BEFORE INSERT OR UPDATE OR DELETE ON public.eindexstru
FOR EACH ROW EXECUTE PROCEDURE public.tr_etl_table_iud_func('EINDEXSTRU');

-- Строка задания (без неё UPDATE в функции ничего не изменит):
--   INSERT INTO etl_user.etl_jobs (tablename, period, last_success_ts, isokaudit)
--   SELECT 'EINDEXSTRU', current_date, clock_timestamp(), 0
--   WHERE NOT EXISTS (SELECT 1 FROM etl_user.etl_jobs
--                     WHERE tablename = 'EINDEXSTRU');

-- Проверка: какие справочники сейчас ждут переноса.
--   SELECT tablename, isokaudit, last_success_ts FROM etl_user.etl_jobs
--   WHERE isokaudit = 0 ORDER BY tablename;
