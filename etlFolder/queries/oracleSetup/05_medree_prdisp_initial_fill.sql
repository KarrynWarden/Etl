--------------------------------------------------------------------------------
-- Medree_prdisp: ПЕРВИЧНОЕ заполнение (разово, вручную).
--
-- Запрос из ТЗ даёт ~16 млн строк — заливаем БАТЧАМИ ПО МЕСЯЦАМ, а не одним
-- INSERT (иначе риск ORA-30036 по UNDO, полный откат при обрыве, не видно
-- прогресса). На каждый месяц — своя транзакция (INSERT + COMMIT).
--
-- КАК ЗАПУСКАТЬ:
--   * Под пользователем-ВЛАДЕЛЬЦЕМ таблиц (koknaev) — тогда все объекты видны
--     напрямую, без ролей. ORA-00942 в PL/SQL как раз означает, что объект не
--     виден текущему пользователю (не тот юзер, либо доступ только через роль,
--     а роли в PL/SQL ненадёжны — нужны ПРЯМЫЕ гранты). Все таблицы ниже — с
--     явной схемой koknaev; если какая-то (spstandardgr/expmed/medcheck) лежит
--     в другой схеме, поправь префикс.
--   * ПРОГРЕСС смотри в таблице-логе koknaev.medree_prdisp_log (пишется
--     автономной транзакцией — виден СРАЗУ, из другой сессии):
--        SELECT * FROM koknaev.medree_prdisp_log ORDER BY ts DESC;
--     DBMS_OUTPUT для этого НЕ годится: его буфер отдаётся только после
--     завершения ВСЕГО блока (а `SET SERVEROUTPUT ON` — команда SQL*Plus, не SQL,
--     в DBeaver/JDBC даёт ORA-00922).
--     Внимание: месяцы, где по фильтрам 0 строк, коммитятся с 0 — в самой
--     medree_prdisp их не видно, поэтому GROUP BY-запрос по таблице может долго
--     показывать пусто. В логе такие месяцы видны явно (0 строк).
--   * ПЕРЕД заливкой удали триггер (для direct-path /*+ APPEND */):
--        DROP TRIGGER koknaev.medree_prdisp_bi;
--     после — создай заново (скрипт 04). idrw берём явно из sequence в SELECT —
--     он монотонно уникален через ВСЕ месяцы, БЕЗ повторов между батчами.
--   * Идемпотентно: залитые месяцы пропускаются (skip-if-exists), скрипт можно
--     перезапускать; фильтр по диапазону date_2 использует индекс.
--
-- ВЫВЕРИТЬ имена схем/колонок под реальную БД (см. также 06_*):
--   koknaev.medree(m): id_rz, code_mes1, date_2, recid, mo, accno, accdt
--   koknaev.spstandardgr(r): medstandard, dbegin, dend, groupcode
--   koknaev.expmed(f): recid, stepexp, svodno
--   koknaev.medcheck: mo, accno, accdt, mekdt
--------------------------------------------------------------------------------

-- Таблица-лог прогресса (создать один раз; безопасно перезапускать скрипт —
-- ошибку «уже существует» глушим).
DECLARE
    e_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_exists, -955);   -- ORA-00955: имя уже занято
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE koknaev.medree_prdisp_log (
            ts    TIMESTAMP DEFAULT SYSTIMESTAMP,
            ym    VARCHAR2(7),
            rows_inserted NUMBER,
            total NUMBER,
            note  VARCHAR2(200)
        )';
EXCEPTION WHEN e_exists THEN NULL;
END;
/

-- Процедура записи прогресса: АВТОНОМНАЯ транзакция — строка лога коммитится
-- сразу и видна из другой сессии, независимо от основной транзакции заливки.
CREATE OR REPLACE PROCEDURE koknaev.medree_prdisp_log_p(
    p_ym VARCHAR2, p_rows NUMBER, p_total NUMBER, p_note VARCHAR2 DEFAULT NULL) AS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO koknaev.medree_prdisp_log (ym, rows_inserted, total, note)
    VALUES (p_ym, p_rows, p_total, p_note);
    COMMIT;
END;
/

DECLARE
    v_month  DATE;
    v_end    DATE;
    v_exists PLS_INTEGER;
    v_cnt    PLS_INTEGER;
    v_total  PLS_INTEGER := 0;
BEGIN
    -- Границы: первый и последний месяц по данным medree.
    SELECT TRUNC(MIN(date_2), 'MM'), TRUNC(MAX(date_2), 'MM')
      INTO v_month, v_end
      FROM koknaev.medree;

    IF v_month IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('medree пуст — нечего заливать');
        RETURN;
    END IF;

    koknaev.medree_prdisp_log_p(NULL, NULL, 0,
        'СТАРТ: месяцы ' || TO_CHAR(v_month, 'YYYY-MM') || ' .. '
        || TO_CHAR(v_end, 'YYYY-MM') || ', всего '
        || (MONTHS_BETWEEN(v_end, v_month) + 1) || ' шт.');

    WHILE v_month <= v_end LOOP
        -- SKIP: месяц уже залит (идемпотентность/возобновление после сбоя).
        SELECT COUNT(*) INTO v_exists
          FROM koknaev.medree_prdisp
         WHERE year  = EXTRACT(YEAR  FROM v_month)
           AND month = EXTRACT(MONTH FROM v_month)
           AND ROWNUM = 1;

        IF v_exists > 0 THEN
            koknaev.medree_prdisp_log_p(TO_CHAR(v_month, 'YYYY-MM'), NULL, v_total,
                                        'уже залит, пропуск');
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_month, 'YYYY-MM') || ' -> уже залит, пропуск');
        ELSE
            -- отметка «начал месяц» — видно, что идёт именно этот период
            koknaev.medree_prdisp_log_p(TO_CHAR(v_month, 'YYYY-MM'), NULL, v_total,
                                        'начал');
            INSERT /*+ APPEND */ INTO koknaev.medree_prdisp (idrw, id, groupcode, year, month)
            SELECT koknaev.medree_prdisp_seq.NEXTVAL,
                   m.id_rz,
                   r.groupcode,
                   EXTRACT(YEAR  FROM m.date_2),
                   EXTRACT(MONTH FROM m.date_2)
            FROM   koknaev.medree m
            JOIN   koknaev.spstandardgr r
                   ON  m.code_mes1 = r.medstandard
                   AND m.date_2 BETWEEN r.dbegin AND r.dend
                   AND r.groupcode IN (1, 2, 3)
            WHERE  m.date_2 >= v_month
              AND  m.date_2 <  ADD_MONTHS(v_month, 1)
              AND  NOT EXISTS (SELECT 1 FROM koknaev.expmed f
                               WHERE f.recid = m.recid
                                 AND f.stepexp = 1
                                 AND f.svodno IS NOT NULL)
              AND  EXISTS     (SELECT 1 FROM koknaev.medcheck c
                               WHERE c.mo = m.mo
                                 AND c.accno = m.accno
                                 AND c.accdt = m.accdt
                                 AND c.mekdt IS NOT NULL);

            v_cnt := SQL%ROWCOUNT;
            v_total := v_total + v_cnt;
            COMMIT;   -- direct-path требует COMMIT перед чтением таблицы на след. итерации

            koknaev.medree_prdisp_log_p(TO_CHAR(v_month, 'YYYY-MM'), v_cnt, v_total,
                                        'готов');
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_month, 'YYYY-MM')
                                 || ' -> ' || v_cnt
                                 || ' строк (итого ' || v_total || ')');
        END IF;

        v_month := ADD_MONTHS(v_month, 1);
    END LOOP;

    koknaev.medree_prdisp_log_p(NULL, NULL, v_total, 'ФИНИШ');
    DBMS_OUTPUT.PUT_LINE('Готово. Всего залито за этот прогон: ' || v_total);
END;
/
