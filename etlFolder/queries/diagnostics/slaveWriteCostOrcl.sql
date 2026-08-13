--------------------------------------------------------------------------------
-- Почему запись в ораклову ведомую стоит столько, сколько стоит.
--
-- Запускать НЕ нам, а DBA: только чтение словаря и динамических представлений,
-- ничего не меняет. Нужен доступ к v$sql / v$session_event.
--
-- Зачем: перенос в режиме массового обновления (isokaudit = 4) на каждую группу
-- делает DELETE всей группы и MERGE её строк заново. Замеры со стороны ETL
-- показали, что цена НЕ определяется ни числом строк, ни формой запроса: две
-- группы iperson почти одного размера (4860 и 4990 строк) дали удаление 1.4с и
-- 21.8с — разница в 15 раз на одном и том же операторе с одним биндом. Дальше
-- со стороны приложения не видно ничего; ответ лежит в плане и в ожиданиях.
--
-- Подставить свои значения:
--   &&owner  — владелец ведомой таблицы (например KOKNAEV)
--   &&table  — имя ведомой таблицы В ВЕРХНЕМ РЕГИСТРЕ (например IPERSON)
--------------------------------------------------------------------------------

DEFINE owner = KOKNAEV
DEFINE table = IPERSON

-- 1. Все индексы таблицы. Каждый из них сопровождается и при удалении строки,
--    и при вставке — то есть умножает цену обеих фаз переноса.
SELECT i.index_name,
       i.uniqueness,
       i.status,
       i.num_rows,
       LISTAGG(c.column_name, ', ')
           WITHIN GROUP (ORDER BY c.column_position) AS columns
FROM   all_indexes i
JOIN   all_ind_columns c
  ON   c.index_owner = i.owner
 AND   c.index_name  = i.index_name
WHERE  i.table_owner = '&&owner'
AND    i.table_name  = '&&table'
GROUP  BY i.index_name, i.uniqueness, i.status, i.num_rows
ORDER  BY i.index_name;

-- 2. Есть ли индекс на колонке группировки. Без него DELETE группы читает
--    таблицу целиком, и тогда его цена почти не зависит от числа строк группы.
SELECT i.index_name, c.column_position, c.column_name
FROM   all_ind_columns c
JOIN   all_indexes i
  ON   i.owner = c.index_owner
 AND   i.index_name = c.index_name
WHERE  c.table_owner = '&&owner'
AND    c.table_name  = '&&table'
AND    c.column_name = 'CREATEDATE'
ORDER  BY i.index_name, c.column_position;

-- 3. Внешние ключи ДРУГИХ таблиц, ссылающиеся на нашу, у которых ведущая
--    колонка ключа не проиндексирована. Классическая причина медленного
--    удаления: Oracle на КАЖДУЮ удаляемую строку родителя просматривает
--    дочернюю таблицу целиком. Цена такого удаления растёт вместе с дочерней
--    таблицей, а не с размером группы.
SELECT fk.owner            AS child_owner,
       fk.table_name       AS child_table,
       fk.constraint_name,
       cc.column_name      AS fk_first_column
FROM   all_constraints fk
JOIN   all_cons_columns cc
  ON   cc.owner = fk.owner
 AND   cc.constraint_name = fk.constraint_name
 AND   cc.position = 1
JOIN   all_constraints pk
  ON   pk.owner = fk.r_owner
 AND   pk.constraint_name = fk.r_constraint_name
WHERE  fk.constraint_type = 'R'
AND    pk.owner      = '&&owner'
AND    pk.table_name = '&&table'
AND    NOT EXISTS (
           SELECT 1
           FROM   all_ind_columns ic
           WHERE  ic.table_owner    = fk.owner
           AND    ic.table_name     = fk.table_name
           AND    ic.column_name    = cc.column_name
           AND    ic.column_position = 1
       )
ORDER  BY fk.owner, fk.table_name;

-- 4. Наши операторы в разделяемом пуле: сколько выполнений, сколько времени на
--    выполнение, сколько блоков читают. Несколько строк с РАЗНЫМИ
--    plan_hash_value на один sql_id означают, что план менялся — это и есть
--    объяснение «один и тот же запрос то 1.4с, то 21.8с».
SELECT sql_id,
       child_number,
       plan_hash_value,
       executions,
       rows_processed,
       ROUND(elapsed_time / 1e6, 1)                          AS elapsed_s,
       ROUND(elapsed_time / GREATEST(executions, 1) / 1e3, 1) AS ms_per_exec,
       buffer_gets,
       disk_reads,
       SUBSTR(sql_text, 1, 80)                                AS sql_head
FROM   v$sql
WHERE  (sql_text LIKE 'DELETE FROM &&table%'
        OR sql_text LIKE 'MERGE INTO &&table%')
ORDER  BY elapsed_time DESC;

-- 5. Фактический план конкретного курсора из пункта 4 — с реальными строками
--    и временем по шагам, а не с оценками оптимизатора.
--    Подставить sql_id и child_number из предыдущего запроса.
-- SELECT * FROM TABLE(dbms_xplan.display_cursor('<sql_id>', <child_number>,
--                                               'ALLSTATS LAST'));

-- 6. На чём ждала сессия переноса. Здесь видно, диски это ('db file sequential
--    read'), журнал ('log file sync') или блокировки ('enq: TX - ...').
--    Подставить sid сессии переноса (v$session по osuser/program/module).
-- SELECT * FROM (
--     SELECT event,
--            total_waits,
--            ROUND(time_waited_micro / 1e6, 1) AS waited_s
--     FROM   v$session_event
--     WHERE  sid = <sid>
--     AND    wait_class <> 'Idle'
--     ORDER  BY time_waited_micro DESC
-- ) WHERE rownum <= 15;

--------------------------------------------------------------------------------
-- Отдельно: MERGE в этой схеме НЕ лишний, убирать его в пользу простого INSERT
-- нельзя, хотя группа перед заливкой и удаляется целиком. Если строка сменила
-- группу, её копия в ведомой лежит под СТАРЫМ периодом и удалением текущей
-- группы не затрагивается; простой INSERT упал бы на ней дублем по первичному
-- ключу, а MERGE переписывает её вместе с периодом.
--------------------------------------------------------------------------------
