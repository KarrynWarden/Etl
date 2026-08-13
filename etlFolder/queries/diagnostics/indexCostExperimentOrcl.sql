--------------------------------------------------------------------------------
-- Эксперимент: сколько из времени перезаливки группы стоят индексы ведомой.
--
-- Повод. У KOKNAEV.IPERSON 21 индекс. Массовое обновление (isokaudit = 4)
-- удаляет группу целиком и вставляет её заново, то есть трогает 42 индексные
-- записи на строку. Замеры со стороны ETL это подтверждают: DELETE группы даёт
-- 27.5 обращений к буферам и 2.3 физических чтения НА СТРОКУ — ровно 21 индекс
-- плюс таблица. Вопрос эксперимента: если индексов будет меньше, во сколько раз
-- станет быстрее.
--
-- Здесь два способа это померить. ВАРИАНТ А ничего не трогает в боевой таблице
-- и ни на кого не влияет — начинать надо с него. ВАРИАНТ Б меряет ровно то же
-- самое на самой IPERSON, но на время эксперимента ломает планы всему
-- остальному софту и требует потом перестроения индексов; он здесь только
-- потому, что даёт цифру на настоящем объёме.
--
-- Скрипт НИЧЕГО не выполняет сам: варианты разделены, операторы изменения
-- собраны в генераторы, чтобы было видно, что именно уйдёт в базу.
--------------------------------------------------------------------------------

DEFINE owner = KOKNAEV
DEFINE tab   = IPERSON

--------------------------------------------------------------------------------
-- ВАРИАНТ А. На копии таблицы. Боевая IPERSON не затрагивается.
--------------------------------------------------------------------------------
-- Смысл: собрать копию из нескольких групп, прогнать на ней ту же пару
-- «удалить группу / вставить её заново» сначала с одним первичным ключом, потом
-- со всеми индексами, и сравнить время. Абсолютные секунды будут меньше боевых
-- (копия меньше, индексы ниже), но ОТНОШЕНИЕ между «1 индекс» и «21 индекс» —
-- то самое, ради чего всё затевается.
--
-- Брать группы покрупнее и побольше, чтобы блоки индексов не помещались в кэш
-- целиком: чем ближе объём копии к боевому, тем честнее цифра.

-- A1. Источник строк (из него будем вставлять) и подопытная таблица.
--     Индексы и ограничения при CREATE AS SELECT не копируются — это то, что
--     нужно: подопытная стартует голой.
CREATE TABLE &&owner..&&tab._IDXSRC AS
SELECT * FROM &&owner..&&tab
WHERE  createdate IN (DATE '2013-02-06', DATE '2013-02-20', DATE '2013-04-04');

CREATE TABLE &&owner..&&tab._IDXTEST AS
SELECT * FROM &&owner..&&tab._IDXSRC;

SELECT COUNT(*) AS rows_in_copy FROM &&owner..&&tab._IDXTEST;

-- A2. Замер БЕЗ индексов вовсе — нижняя граница, чистая цена таблицы.
SET TIMING ON
DELETE FROM &&owner..&&tab._IDXTEST WHERE createdate = DATE '2013-02-20';
INSERT INTO &&owner..&&tab._IDXTEST
SELECT * FROM &&owner..&&tab._IDXSRC WHERE createdate = DATE '2013-02-20';
COMMIT;

-- A3. Только первичный ключ — так выглядела бы ведомая, будь она честной копией
--     без обслуживания чужих запросов.
ALTER TABLE &&owner..&&tab._IDXTEST
    ADD CONSTRAINT PK_&&tab._IDXTEST PRIMARY KEY (IDROW);
CREATE INDEX &&owner..IX_&&tab._IDXTEST_CD ON &&owner..&&tab._IDXTEST (CREATEDATE);

DELETE FROM &&owner..&&tab._IDXTEST WHERE createdate = DATE '2013-02-20';
INSERT INTO &&owner..&&tab._IDXTEST
SELECT * FROM &&owner..&&tab._IDXSRC WHERE createdate = DATE '2013-02-20';
COMMIT;

-- A4. Теперь навесить ОСТАЛЬНЫЕ индексы, как на боевой, и повторить тот же
--     замер. Операторы создания генерируются из словаря — переносим один в один,
--     только с другим именем таблицы и индекса.
--     (Функциональный индекс на SYS_NC00065$ этот генератор пропустит: он лежит
--     на скрытой колонке, его надо переписать руками по выражению из
--     all_ind_expressions — см. запрос A5.)
SELECT 'CREATE INDEX &&owner..X' || ROWNUM || '_IDXTEST ON &&owner..&&tab._IDXTEST ('
       || LISTAGG(c.column_name, ', ')
              WITHIN GROUP (ORDER BY c.column_position) || ');' AS stmt
FROM   all_indexes i
JOIN   all_ind_columns c
  ON   c.index_owner = i.owner AND c.index_name = i.index_name
WHERE  i.table_owner = '&&owner'
AND    i.table_name  = '&&tab'
AND    i.index_name NOT IN ('PK_&&tab')
AND    i.index_type = 'NORMAL'
GROUP  BY i.index_name
ORDER  BY i.index_name;

-- A5. Выражения функциональных индексов (если их надо воспроизвести точно).
SELECT i.index_name, e.column_position, e.column_expression
FROM   all_indexes i
JOIN   all_ind_expressions e
  ON   e.index_owner = i.owner AND e.index_name = i.index_name
WHERE  i.table_owner = '&&owner'
AND    i.table_name  = '&&tab'
ORDER  BY i.index_name, e.column_position;

-- A6. Тот же замер на полном комплекте индексов. Разница с A3 и есть ответ.
DELETE FROM &&owner..&&tab._IDXTEST WHERE createdate = DATE '2013-02-20';
INSERT INTO &&owner..&&tab._IDXTEST
SELECT * FROM &&owner..&&tab._IDXSRC WHERE createdate = DATE '2013-02-20';
COMMIT;
SET TIMING OFF

-- A7. Убрать за собой.
-- DROP TABLE &&owner..&&tab._IDXTEST PURGE;
-- DROP TABLE &&owner..&&tab._IDXSRC  PURGE;


--------------------------------------------------------------------------------
-- ВАРИАНТ Б. На самой IPERSON. Только в окно обслуживания.
--------------------------------------------------------------------------------
-- Что происходит: индекс переводится в UNUSABLE, и Oracle перестаёт его
-- сопровождать при DML — ровно то, что мы хотим померить. Цена:
--
--   * пока индекс UNUSABLE, оптимизатор его не видит. Любой запрос любого
--     софта, который на него опирался, уходит в полный просмотр 28 млн строк.
--     Это и есть то, чего Вы опасались, — и опасение верное;
--   * вернуть индекс = ALTER INDEX ... REBUILD, а это построение заново по всем
--     28 млн строк, не мгновенное. Планировать надо вместе с окном;
--   * УНИКАЛЬНЫЕ индексы трогать НЕЛЬЗЯ: PK_IPERSON и IPERSON001. Неработающий
--     уникальный индекс роняет любой DML по таблице (ORA-01502), а не просто
--     замедляет чтение. Генератор ниже их исключает;
--   * ИНДЕКС ПО CREATEDATE (IPERSON019) ТОЖЕ ТРОГАТЬ НЕЛЬЗЯ. По нему идёт
--     DELETE группы. Без него удаление станет полным просмотром таблицы, и
--     эксперимент померяет не то, что задуман. Генератор исключает и его.
--
-- ВАЖНО заранее: убедиться, что пропуск неработающих индексов включён. Если
-- параметр FALSE, DML по таблице с UNUSABLE-индексом не замедлится, а упадёт.
SELECT name, value FROM v$parameter WHERE name = 'skip_unusable_indexes';

-- Б1. Что вообще есть, с типом и уникальностью — глазами, перед тем как что-то
--     отключать.
SELECT i.index_name, i.uniqueness, i.index_type, i.status,
       LISTAGG(c.column_name, ', ')
           WITHIN GROUP (ORDER BY c.column_position) AS columns,
       (SELECT COUNT(*) FROM all_constraints k
        WHERE k.owner = i.owner AND k.index_name = i.index_name) AS backs_constraint
FROM   all_indexes i
JOIN   all_ind_columns c
  ON   c.index_owner = i.owner AND c.index_name = i.index_name
WHERE  i.table_owner = '&&owner'
AND    i.table_name  = '&&tab'
GROUP  BY i.owner, i.index_name, i.uniqueness, i.index_type, i.status
ORDER  BY i.index_name;

-- Б2. Генератор отключения. Уникальные, поддерживающие ограничения и индекс по
--     колонке группировки исключены. Прочитать выдачу, выкинуть из неё те, без
--     которых точно нельзя, и только потом выполнять.
SELECT 'ALTER INDEX ' || i.owner || '.' || i.index_name || ' UNUSABLE;' AS stmt
FROM   all_indexes i
WHERE  i.table_owner = '&&owner'
AND    i.table_name  = '&&tab'
AND    i.uniqueness  = 'NONUNIQUE'
AND    i.status      = 'VALID'
AND    NOT EXISTS (SELECT 1 FROM all_constraints k
                   WHERE k.owner = i.owner AND k.index_name = i.index_name)
AND    NOT EXISTS (SELECT 1 FROM all_ind_columns c
                   WHERE c.index_owner = i.owner
                   AND   c.index_name  = i.index_name
                   AND   c.column_name = 'CREATEDATE')
ORDER  BY i.index_name;

-- Б3. Прогнать даг на нескольких группах и снять те же строки лога:
--       Ведомая: удалено N строк группы ...
--       Заливка: N строк, K пачек ..., Xс (Y мс/строка); пачки: первая ...
--     Сравнивать надо мс/строка, а не итог: группы разного размера.

-- Б4. Вернуть всё как было. Список берётся по статусу, так что ничего
--     записывать заранее не нужно. ONLINE — чтобы не блокировать читателей
--     (требует Enterprise Edition; без неё убрать слово, но тогда таблица
--     блокируется на время построения).
SELECT 'ALTER INDEX ' || owner || '.' || index_name || ' REBUILD ONLINE;' AS stmt
FROM   all_indexes
WHERE  table_owner = '&&owner'
AND    table_name  = '&&tab'
AND    status      = 'UNUSABLE'
ORDER  BY index_name;

-- Б5. Проверка, что не осталось ни одного неработающего.
SELECT index_name, status
FROM   all_indexes
WHERE  table_owner = '&&owner'
AND    table_name  = '&&tab'
AND    status <> 'VALID';
