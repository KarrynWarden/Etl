--------------------------------------------------------------------------------
-- Эксперимент: сколько из времени перезаливки группы стоят индексы ведомой.
--
-- ТОЛЬКО ДЛЯ ТЕСТОВОЙ БАЗЫ. Скрипт переводит индексы в UNUSABLE, и пока они в
-- этом состоянии, оптимизатор их не видит: любой запрос, который на них
-- опирался, уходит в полный просмотр таблицы. На боевой так делать нельзя.
--
-- Повод. У KOKNAEV.IPERSON 21 индекс. Массовое обновление (isokaudit = 4)
-- удаляет группу целиком и вставляет её заново, то есть трогает 42 индексные
-- записи на строку. Замеры со стороны ETL это подтверждают: DELETE группы даёт
-- 27.5 обращений к буферам и 2.3 физических чтения НА СТРОКУ — ровно 21 индекс
-- плюс таблица. Вопрос эксперимента: во сколько раз станет быстрее, если
-- индексов будет меньше.
--
-- Идея: UNUSABLE-индекс Oracle при DML не сопровождает. Значит достаточно
-- отключить лишние, прогнать даг на ТЕХ ЖЕ группах и сравнить мс/строка из
-- лога. Перезаливка группы идемпотентна, поэтому один и тот же период можно
-- гонять сколько угодно раз.
--------------------------------------------------------------------------------

DEFINE owner = KOKNAEV
DEFINE tab   = IPERSON

SET SERVEROUTPUT ON SIZE UNLIMITED
SET TIMING ON

--------------------------------------------------------------------------------
-- 0. Подготовка
--------------------------------------------------------------------------------

-- 0.1 Пропуск неработающих индексов обязан быть включён. Если здесь FALSE, то
--     DML по таблице с UNUSABLE-индексом не замедлится, а УПАДЁТ.
SELECT name, value FROM v$parameter WHERE name = 'skip_unusable_indexes';
-- При необходимости на время эксперимента:
--   ALTER SYSTEM SET skip_unusable_indexes = TRUE;

-- 0.2 Что есть сейчас. Смотреть глазами перед тем, как что-то отключать.
--     status = 'N/A' означает секционированный индекс — с ними этот скрипт не
--     работает, у них состояние хранится по секциям (all_ind_partitions).
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

-- 0.3 Замер ДО. Выбрать 2-3 группы покрупнее, прогнать даг и записать из лога
--     мс/строка отдельно по удалению и по заливке:
--
--       Ведомая: удалено 16474 строк группы 2013-02-20
--       Заливка: 16474 строк, 17 пачек по 1000, ..., 121.7с (7.39 мс/строка);
--                пачки: первая 0.9с, последняя 0.2с, самая долгая 89.3с
--
--     Прогнать ДВАЖДЫ и брать второй прогон: первый греет буферный кэш, и без
--     этого сравнение поедет само по себе.

--------------------------------------------------------------------------------
-- 1. Отключить лишние индексы
--------------------------------------------------------------------------------
-- Блок сам исключает то, что трогать нельзя:
--   * УНИКАЛЬНЫЕ (PK_IPERSON, IPERSON001). Неработающий уникальный индекс не
--     замедляет чтение, а роняет ЛЮБОЙ DML по таблице с ORA-01502;
--   * поддерживающие ограничения;
--   * индекс по CREATEDATE (IPERSON019) — по нему идёт сам DELETE группы. Без
--     него удаление станет полным просмотром таблицы, и эксперимент померяет
--     не то, что задуман.
-- Каждый оператор печатается перед выполнением, так что в спуле остаётся
-- полный список того, что было отключено.

BEGIN
    FOR r IN (
        SELECT i.owner, i.index_name
        FROM   all_indexes i
        WHERE  i.table_owner = '&&owner'
        AND    i.table_name  = '&&tab'
        AND    i.uniqueness  = 'NONUNIQUE'
        AND    i.status      = 'VALID'
        AND    NOT EXISTS (SELECT 1 FROM all_constraints k
                           WHERE k.owner = i.owner
                           AND   k.index_name = i.index_name)
        AND    NOT EXISTS (SELECT 1 FROM all_ind_columns c
                           WHERE c.index_owner = i.owner
                           AND   c.index_name  = i.index_name
                           AND   c.column_name = 'CREATEDATE')
        ORDER  BY i.index_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('ALTER INDEX ' || r.owner || '.'
                             || r.index_name || ' UNUSABLE;');
        EXECUTE IMMEDIATE 'ALTER INDEX ' || r.owner || '.'
                          || r.index_name || ' UNUSABLE';
    END LOOP;
END;
/

-- Сколько осталось работающих — столько и будет сопровождаться при записи.
SELECT status, COUNT(*) AS indexes
FROM   all_indexes
WHERE  table_owner = '&&owner' AND table_name = '&&tab'
GROUP  BY status;

--------------------------------------------------------------------------------
-- 2. Замер ПОСЛЕ
--------------------------------------------------------------------------------
-- Прогнать даг на ТЕХ ЖЕ группах, тоже дважды, и сравнить мс/строка.
-- Сравнивать надо именно мс/строка, а не итог: группы разного размера.
--
-- Заодно посмотреть, исчезли ли одиночные остановки в 90 секунд («самая долгая
-- пачка» в строке лога). Если исчезли — они были от redo, который порождало
-- сопровождение индексов; если остались — причина отдельная, и дальше идёт
-- запрос 7 из slaveWriteCostOrcl.sql про размер и частоту переключения журналов.

-- Свежая цена наших операторов по словарю (сбросить статистику нельзя, поэтому
-- смотреть на ms_per_exec у курсоров, появившихся после отключения).
SELECT sql_id, child_number, plan_hash_value, executions, rows_processed,
       ROUND(elapsed_time / 1e6, 1)                           AS elapsed_s,
       ROUND(elapsed_time / GREATEST(executions, 1) / 1e3, 1) AS ms_per_exec,
       buffer_gets, disk_reads,
       TO_CHAR(last_active_time, 'YYYY-MM-DD HH24:MI:SS')     AS last_active
FROM   v$sql
WHERE  (sql_text LIKE 'DELETE FROM &&tab%' OR sql_text LIKE 'MERGE INTO &&tab%')
ORDER  BY last_active_time DESC;

--------------------------------------------------------------------------------
-- 3. Вернуть всё как было
--------------------------------------------------------------------------------
-- Список берётся по статусу, записывать заранее ничего не нужно.
--
-- PARALLEL + NOLOGGING — чтобы построение 19 индексов по 28 млн строк не заняло
-- полночи; на тестовой базе это допустимо (NOLOGGING делает индекс
-- невосстановимым из бэкапа до следующей копии).
--
-- ОБЯЗАТЕЛЬНО следом NOPARALLEL: степень параллелизма остаётся АТРИБУТОМ
-- индекса после REBUILD и потом молча меняет планы запросов — классическая
-- ловушка, из-за которой «после перестроения база стала вести себя странно».

BEGIN
    FOR r IN (
        SELECT owner, index_name
        FROM   all_indexes
        WHERE  table_owner = '&&owner'
        AND    table_name  = '&&tab'
        AND    status      = 'UNUSABLE'
        ORDER  BY index_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('REBUILD ' || r.index_name || ' ...');
        EXECUTE IMMEDIATE 'ALTER INDEX ' || r.owner || '.' || r.index_name
                          || ' REBUILD PARALLEL 4 NOLOGGING';
        EXECUTE IMMEDIATE 'ALTER INDEX ' || r.owner || '.' || r.index_name
                          || ' NOPARALLEL LOGGING';
    END LOOP;
END;
/

-- Проверка: неработающих не осталось, параллелизм сброшен.
SELECT index_name, status, degree
FROM   all_indexes
WHERE  table_owner = '&&owner' AND table_name = '&&tab'
AND    (status <> 'VALID' OR TRIM(degree) NOT IN ('1', 'DEFAULT'))
ORDER  BY index_name;

SET TIMING OFF

--------------------------------------------------------------------------------
-- Промежуточные шаги, если понадобится кривая, а не две точки
--------------------------------------------------------------------------------
-- Вернуть часть индексов и померить снова — тогда видно, линейно ли растёт цена
-- с их числом. Имена подставить свои:
--
--   ALTER INDEX &&owner..IPERSON002 REBUILD PARALLEL 4 NOLOGGING;
--   ALTER INDEX &&owner..IPERSON002 NOPARALLEL LOGGING;
--
-- Обратно в UNUSABLE — по одному индексу, мгновенно:
--
--   ALTER INDEX &&owner..IPERSON002 UNUSABLE;
