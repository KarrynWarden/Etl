--------------------------------------------------------------------------------
-- Medree_prdisp: ПЕРВИЧНОЕ заполнение (разово, вручную).
--
-- Таблица сейчас пустая. Запрос из ТЗ даёт ~16 млн строк — заливать их ОДНИМ
-- INSERT ... SELECT НЕ надо: вся вставка была бы одной транзакцией (риск
-- ORA-30036 по UNDO, полный откат при обрыве сессии, не видно прогресса,
-- построчный триггер idrw на 16 млн). Поэтому льём БАТЧАМИ ПО МЕСЯЦАМ:
--   * один батч = один календарный месяц (крупнейшие группы ~400-429 тыс строк —
--     для UNDO комфортно);
--   * COMMIT после каждого месяца → UNDO не копится, при сбое теряется максимум
--     текущий месяц;
--   * DBMS_OUTPUT печатает прогресс (месяц → сколько строк);
--   * фильтр по ДИАПАЗОНУ date_2 (sargable — использует индекс на medree.date_2),
--     а не по EXTRACT(...);
--   * DELETE месяца перед INSERT → идемпотентно: скрипт можно перезапустить с
--     любого места, дублей не будет;
--   * idrw берём из medree_prdisp_seq.NEXTVAL прямо в SELECT — триггер
--     medree_prdisp_bi (WHEN idrw IS NULL) при этом НЕ срабатывает, вставка
--     остаётся set-based (по месяцу), без построчного триггера на 16 млн.
--
-- ПЕРЕД запуском: SET SERVEROUTPUT ON  (иначе прогресс не увидишь).
-- Оценить объём заранее (по месяцам) можно запросом из ТЗ с GROUP BY year, month.
--
-- ВЫВЕРИТЬ на тесте имена схем/колонок (см. также 06_*):
--   koknaev.medree(m): id_rz, code_mes1, date_2, recid, mo, accno, accdt
--   spstandardgr(r):   medstandard, dbegin, dend, groupcode
--   expmed(f):         recid, stepexp, svodno
--   medcheck:          mo, accno, accdt, mekdt
--------------------------------------------------------------------------------

SET SERVEROUTPUT ON

DECLARE
    v_month  DATE;
    v_end    DATE;
    v_cnt    PLS_INTEGER;
    v_total  PLS_INTEGER := 0;
BEGIN
    -- Границы: первый и последний месяц по данным medree (min/max по индексу).
    SELECT TRUNC(MIN(date_2), 'MM'), TRUNC(MAX(date_2), 'MM')
      INTO v_month, v_end
      FROM koknaev.medree;

    IF v_month IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('medree пуст — нечего заливать');
        RETURN;
    END IF;

    WHILE v_month <= v_end LOOP
        -- идемпотентность: перезалить месяц целиком
        DELETE FROM Medree_prdisp
         WHERE year  = EXTRACT(YEAR  FROM v_month)
           AND month = EXTRACT(MONTH FROM v_month);

        INSERT INTO Medree_prdisp (idrw, id, groupcode, year, month)
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
        COMMIT;   -- фиксируем каждый месяц: UNDO не растёт, прогресс сохранён

        DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_month, 'YYYY-MM')
                             || ' -> ' || v_cnt
                             || ' строк (итого ' || v_total || ')');

        v_month := ADD_MONTHS(v_month, 1);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Готово. Всего залито: ' || v_total);
END;
/
