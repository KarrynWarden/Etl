--------------------------------------------------------------------------------
-- iprkdept: колонка createdate и ключ idrw на ВЕДОМОЙ стороне (PostgreSQL).
--
-- Пара к oracleSetup/11_iprkdept_createdate.sql — там же объяснено, зачем
-- линия переезжает с section_compare по dbeg на iud по createdate и почему
-- ключом становится idrw. Запускать ПОСЛЕ оракловой части и ДО выкладки кода.
--
-- createdate заполняется тем же правилом, что и на ведущей — днём lastupdate.
-- Это важно: значения обязаны совпасть с точностью до строки, иначе первый же
-- аудит объявит расхождением всю таблицу. Совпадут они потому, что сам
-- lastupdate уже перенесён ETL и на обеих сторонах одинаков.
--
-- Скрипт идемпотентный.
--------------------------------------------------------------------------------

-- 0. Проверки перед началом — обе цифры должны быть 0.
SELECT count(*) AS idrw_null FROM iprkdept WHERE idrw IS NULL;

SELECT count(*) AS idrw_dup FROM (
    SELECT idrw FROM iprkdept GROUP BY idrw HAVING count(*) > 1
) d;


-- 1. Колонка. Тип date (без времени) — на ведущей туда пишется TRUNC(...),
--    времени там нет ни у старых строк, ни у новых.
ALTER TABLE iprkdept ADD COLUMN IF NOT EXISTS createdate date;


-- 2. Заполнение существующих строк — то же правило, что на ведущей.
--    lastupdate здесь timestamp; приведение к date и есть TRUNC.
UPDATE iprkdept SET createdate = lastupdate::date WHERE createdate IS NULL;


-- 3. Индекс по периоду: выборка группы, удаление группы, аудит.
CREATE INDEX IF NOT EXISTS ix_iprkdept_createdate ON iprkdept (createdate);


-- 4. Уникальность idrw — ОБЯЗАТЕЛЬНА.
--
--    В режиме iud запись идёт `INSERT ... ON CONFLICT (idrw) DO UPDATE`, а
--    удаление — `DELETE WHERE idrw = ?`. ON CONFLICT требует именно уникального
--    индекса по этой колонке; без него линия упадёт на первой же строке с
--    «there is no unique or exclusion constraint matching the ON CONFLICT
--    specification».
--
--    NOT NULL нужен отдельно: уникальный индекс сам по себе NULL'ы допускает, и
--    несколько строк с пустым idrw в него поместились бы, а ETL их адресовать
--    не может.
ALTER TABLE iprkdept ALTER COLUMN idrw SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_iprkdept_idrw ON iprkdept (idrw);

-- Старый первичный ключ (dbeg, id, typeprk) НЕ трогаем: он ничему не мешает,
-- а его снятие — отдельное решение, которое лучше принимать не в момент
-- переключения режима переноса.


-- 5. Статистика — планировщик должен узнать про новую колонку и индексы.
ANALYZE iprkdept;


-- 6. Сверка с json-эталоном (structures/IPRKDEPT/iprkdept.json). Ровно этот
--    запрос выполняет проверка структуры перед каждым прогоном; расхождение —
--    это FLK и остановка линии.
--
--    Ожидается, что появилась строка createdate | date | (пусто), а признак
--    первичного ключа переехал на idrw.
SELECT c.column_name, c.data_type, c.numeric_scale
  FROM information_schema.columns c
 WHERE c.table_name = 'iprkdept'
 ORDER BY c.column_name;
