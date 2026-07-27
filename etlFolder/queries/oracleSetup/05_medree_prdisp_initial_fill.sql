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
--   * DBMS_OUTPUT: команда `SET SERVEROUTPUT ON` — это SQL*Plus, НЕ SQL (в
--     DBeaver/JDBC даёт ORA-00922). В SQL*Plus выполни её отдельно перед блоком;
--     в DBeaver включи серверный вывод кнопкой. Прогресс также видно запросом
--     `SELECT year, month, COUNT(*) FROM koknaev.medree_prdisp GROUP BY ...`
--     из другой сессии.
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

    WHILE v_month <= v_end LOOP
        -- SKIP: месяц уже залит (идемпотентность/возобновление после сбоя).
        SELECT COUNT(*) INTO v_exists
          FROM koknaev.medree_prdisp
         WHERE year  = EXTRACT(YEAR  FROM v_month)
           AND month = EXTRACT(MONTH FROM v_month)
           AND ROWNUM = 1;

        IF v_exists > 0 THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_month, 'YYYY-MM') || ' -> уже залит, пропуск');
        ELSE
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

            DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_month, 'YYYY-MM')
                                 || ' -> ' || v_cnt
                                 || ' строк (итого ' || v_total || ')');
        END IF;

        v_month := ADD_MONTHS(v_month, 1);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Готово. Всего залито за этот прогон: ' || v_total);
END;
/
