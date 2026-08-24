--------------------------------------------------------------------------------
-- iprkdept: колонка createdate, ключ IDRW и перевод линии в режим iud (Oracle).
--
-- Что меняется и почему.
--   Линия работала в режиме section_compare с группировкой по DBEG. DBEG — это
--   бизнес-дата начала, а не отметка о заведении строки, и в первичный ключ она
--   входила «пережитком прошлого». Для режима iud нужны две вещи: отдельная
--   колонка периода, которая НЕ меняется, и ключ, который триггер может положить
--   в журнал одним значением.
--
--   Ключ переезжает на IDRW. Составной ключ (DBEG, ID, TYPEPRK) для журнала не
--   годится: триггер склеивает его в строку через TO_CHAR, а TO_CHAR от DATE без
--   маски берёт NLS_DATE_FORMAT СЕССИИ. Сессия триггера (клиент) и сессия
--   переноса (Airflow) могут его не разделять — получили бы ORA-01861 на каждой
--   строке либо, того хуже, другую дату. С IDRW этой развилки нет вовсе.
--
--   createdate у существующих строк заполняется TRUNC(LASTUPDATE) — решение
--   принято при постановке задачи. Строки с пустым lastupdate попадут в
--   NULL-группу; это полноправная группа (см. README, «NULL-период»), но их
--   количество лучше знать заранее — запрос ниже.
--
--   ВРЕМЕНИ В createdate НЕТ (везде TRUNC), поэтому у линии снимается
--   truncatePeriod: условие периода становится `createdate = :период` вместо
--   `TRUNC(createdate) = :период`, то есть перестаёт прятать колонку под функцию
--   и снова использует индекс.
--
-- ПОРЯДОК. Строго такой, иначе линия встанет с FLK:
--   1) этот скрипт (Oracle);
--   2) postgresSetup/04_iprkdept_slave.sql;
--   3) только теперь выкладывать код — структуры и конфиг из репозитория ждут
--      колонку с обеих сторон. Выложенные раньше — сверка структур не сойдётся.
--------------------------------------------------------------------------------

-- 0. ПРОВЕРКИ ПЕРЕД НАЧАЛОМ. Выполнить и посмотреть глазами.

-- 0.1 IDRW обязан быть уникальным и непустым — на нём будет держаться весь
--     точечный перенос. Обе цифры должны быть 0.
SELECT COUNT(*) AS idrw_null
  FROM koknaev.iprkdept WHERE idrw IS NULL;

SELECT COUNT(*) AS idrw_dup FROM (
    SELECT idrw FROM koknaev.iprkdept GROUP BY idrw HAVING COUNT(*) > 1
);

-- 0.2 Сколько строк уедет в NULL-группу (lastupdate не заполнен).
SELECT COUNT(*) AS lastupdate_null
  FROM koknaev.iprkdept WHERE lastupdate IS NULL;


-- 1. Колонка.
ALTER TABLE koknaev.iprkdept ADD (createdate DATE);


-- 2. Заполнение существующих строк.
--    На большой таблице это долгий UPDATE в одной транзакции — если не
--    укладывается по undo, дробить по годам:
--        UPDATE koknaev.iprkdept SET createdate = TRUNC(lastupdate)
--        WHERE createdate IS NULL
--          AND lastupdate >= DATE '2020-01-01' AND lastupdate < DATE '2021-01-01';
--        COMMIT;
UPDATE koknaev.iprkdept SET createdate = TRUNC(lastupdate);
COMMIT;


-- 3. Индекс по периоду — по нему идут выборка группы, удаление группы и аудит.
CREATE INDEX koknaev.iprkdept_createdate_ix ON koknaev.iprkdept (createdate);

-- 4. Уникальность IDRW. Если запросы 0.1 дали нули — включить; иначе сперва
--    разобраться с данными, потому что перенос по неуникальному ключу будет
--    молча терять строки.
CREATE UNIQUE INDEX koknaev.iprkdept_idrw_uq ON koknaev.iprkdept (idrw);


-- 5. createdate для НОВЫХ строк.
--
--    Приложение, которое пишет в iprkdept, про эту колонку не знает и значение
--    не передаст — без заполнения все новые строки уходили бы в NULL-группу.
--    Триггер BEFORE INSERT, а не DEFAULT: на 11g DEFAULT не срабатывает, когда
--    колонку передали явным NULL, а DEFAULT ON NULL там ещё нет.
--
--    TRUNC(SYSDATE) — без времени, ради sargable-условия периода (см. шапку).
--
--    BEFORE отрабатывает раньше AFTER, поэтому журнальный триггер из шага 6
--    видит уже заполненный createdate.
CREATE OR REPLACE TRIGGER koknaev.iprkdept_bi
BEFORE INSERT ON koknaev.iprkdept
FOR EACH ROW
WHEN (NEW.createdate IS NULL)
BEGIN
    :NEW.createdate := TRUNC(SYSDATE);
END;
/


-- 5.1 Права переносу. Ходит он под etl_user, таблица принадлежит koknaev —
--     без гранта линия падает на ORA-00942 «table or view does not exist», и по
--     тексту ошибки не догадаться, что дело в правах, а не в имени таблицы.
--     SELECT достаточно: iprkdept здесь ведущая, перенос её только читает.
GRANT SELECT ON koknaev.iprkdept TO etl_user;


-- 6. Журнальный триггер линии.
--    Текст целиком: etlFolder/queries/triggers/iprkdeptOrclPost.sql
--    Ставить ДО того, как линия переключится в режим iud, иначе изменения,
--    сделанные в промежутке, в журнал не попадут и уедут только следующим
--    полным сравнением.
--
--    ВНИМАНИЕ к регистру: линия называется 'iprkdept' строчными (так стоит
--    tableNameEtlJobs в даге), и триггер пишет в журнал ровно эту строку.
--    Сравнение с etl_jobs.tablename регистрозависимое.
@@../triggers/iprkdeptOrclPost.sql


-- 7. Проверка. Сделать любое изменение и убедиться, что запись появилась.
--
--    UPDATE koknaev.iprkdept SET lastupdate = lastupdate WHERE idrw = <любой>;
--    COMMIT;
SELECT tablename, oper, period, id, isetl, timeoper
  FROM koknaev.etl_log_iud_row
 WHERE tablename = 'iprkdept'
 ORDER BY idrw DESC FETCH FIRST 10 ROWS ONLY;

-- Пустых периодов в самой таблице (эти строки поедут NULL-группой):
SELECT COUNT(*) AS createdate_null FROM koknaev.iprkdept WHERE createdate IS NULL;
