--------------------------------------------------------------------------------
-- Medree_prdisp: ПЕРВИЧНОЕ заполнение (разово, вручную, в SQL-редакторе).
--
-- Запрос из ТЗ даёт ~16 млн строк — заливаем БАТЧАМИ ПО МЕСЯЦАМ, а не одним
-- INSERT (иначе риск ORA-30036 по UNDO, полный откат при обрыве, не видно
-- прогресса). На каждый месяц — своя транзакция (INSERT + COMMIT).
--
-- ПЕРЕД запуском:
--   1) SET SERVEROUTPUT ON SIZE UNLIMITED   -- иначе не увидишь прогресс
--   2) УДАЛИ триггер, чтобы работал direct-path /*+ APPEND */ (Oracle молча
--      деградирует APPEND в обычный INSERT, если на таблице есть триггер):
--          DROP TRIGGER medree_prdisp_bi;
--      После заливки СОЗДАЙ его заново (скрипт 04_medree_prdisp_idrw.sql) —
--      он нужен регулярному джобу (06).
--
-- Почему это безопасно:
--   * idrw берём ЯВНО из medree_prdisp_seq.NEXTVAL в SELECT. Sequence монотонно
--     уникальна через ВСЕ месяцы/транзакции — idrw НЕ ПОВТОРЯЕТСЯ между месяцами
--     (CACHE может дать пропуски при рестарте инстанса, но не дубли — для
--     суррогатного ключа это неважно). Триггер для заливки не нужен.
--   * INSERT /*+ APPEND */ — direct-path, быстро и с малым UNDO. БЕЗ NOLOGGING
--     (REDO пишется — безопасно для recovery/standby).
--   * Идемпотентность — через SKIP уже залитых месяцев (а не DELETE: DELETE в
--     одной транзакции с APPEND сломал бы direct-path). Скрипт можно
--     перезапускать: залитые месяцы пропускаются, оборванный (откачённый) —
--     дольётся.
--   * Фильтр по ДИАПАЗОНУ date_2 (sargable — использует индекс на medree.date_2).
--
-- ВЫВЕРИТЬ на тесте имена схем/колонок (см. также 06_*):
--   koknaev.medree(m): id_rz, code_mes1, date_2, recid, mo, accno, accdt
--   spstandardgr(r):   medstandard, dbegin, dend, groupcode
--   expmed(f):         recid, stepexp, svodno
--   medcheck:          mo, accno, accdt, mekdt
--------------------------------------------------------------------------------

SET SERVEROUTPUT ON SIZE UNLIMITED

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
          FROM Medree_prdisp
         WHERE year  = EXTRACT(YEAR  FROM v_month)
           AND month = EXTRACT(MONTH FROM v_month)
           AND ROWNUM = 1;

        IF v_exists > 0 THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_month, 'YYYY-MM') || ' -> уже залит, пропуск');
        ELSE
            INSERT /*+ APPEND */ INTO Medree_prdisp (idrw, id, groupcode, year, month)
            SELECT medree_prdisp_seq.NEXTVAL,
                   m.id_rz,
                   r.groupcode,
                   EXTRACT(YEAR  FROM m.date_2),
                   EXTRACT(MONTH FROM m.date_2)
            FROM   koknaev.medree m
            JOIN   spstandardgr r
                   ON  m.code_mes1 = r.medstandard
                   AND m.date_2 BETWEEN r.dbegin AND r.dend
                   AND r.groupcode IN (1, 2, 3)
            WHERE  m.date_2 >= v_month
              AND  m.date_2 <  ADD_MONTHS(v_month, 1)
              AND  NOT EXISTS (SELECT 1 FROM expmed f
                               WHERE f.recid = m.recid
                                 AND f.stepexp = 1
                                 AND f.svodno IS NOT NULL)
              AND  EXISTS     (SELECT 1 FROM medcheck c
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
