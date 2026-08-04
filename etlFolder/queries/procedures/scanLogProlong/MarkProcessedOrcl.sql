-- Закрыть заявки ключа в журнале Oracle: 'OK' — процедура отработала,
-- 'ERR' — упала (разбор по public.ias_func_errlog, idfunc = 286).
--
-- Закрываются ВСЕ открытые заявки ключа, а не только та, что попала в выборку:
-- продление применяется на текущую дату, поэтому более старые заявки того же
-- подразделения уже неактуальны. Так же вела себя прежняя процедура на Postgres.
--
-- lastupdate проставляем явно: на LOG_EINDPROLONG висит только BEFORE INSERT
-- триггер, при UPDATE поле само не обновится.
UPDATE log_eindprolong
   SET dwork = SYSDATE,
       status = :status,
       lastupdate = SYSDATE
 WHERE dwork IS NULL
   AND mo = :mo
   AND otdel = :otdel
   AND dept = :dept
   AND subdept = :subdept
