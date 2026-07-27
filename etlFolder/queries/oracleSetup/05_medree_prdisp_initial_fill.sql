--------------------------------------------------------------------------------
-- Medree_prdisp: ПЕРВИЧНОЕ заполнение (разово, вручную). ОДНИМ ПРОХОДОМ.
--
-- ПОЧЕМУ НЕ ПОМЕСЯЧНО (важно — первая версия скрипта была помесячной и это
-- оказалось ошибкой). Реальный план на этой БД:
--     TABLE ACCESS FULL  MEDCHECK        (199K)
--     INDEX RANGE SCAN   MEDREE001       (по MO/ACCNO/ACCDT)
--     TABLE ACCESS BY INDEX ROWID MEDREE -> filter(DATE_2 >= :B1 AND < ...)
--     TABLE ACCESS FULL  EXPMED          (762K)
-- Предикат по DATE_2 НЕ является driving (индекса на MEDREE.DATE_2 нет): оптимизатор
-- гонит от MEDCHECK, делает ~600 тыс. обращений в MEDREE по индексу и только ПОТОМ
-- отбрасывает строки не своего месяца. То есть КАЖДЫЙ месяц стоит почти как весь
-- запрос целиком -> помесячный цикл умножает работу примерно в 180 раз (число
-- месяцев), а не делит её. Один проход выполняет ту же работу ОДИН раз, хеш-джойнами.
--
-- UNDO при этом не страшен: /*+ APPEND */ — direct-path, пишет над HWM и почти не
-- генерирует undo (опасность ORA-30036 относится к обычному INSERT). REDO пишется
-- (без NOLOGGING) — безопасно для recovery/standby.
--
-- КАК ЗАПУСКАТЬ:
--   1) Под ВЛАДЕЛЬЦЕМ таблиц (koknaev) — схемы указаны явно; роли внутри PL/SQL не
--      действуют, иначе ORA-00942.
--   2) Снять триггер (иначе APPEND молча деградирует в обычный INSERT):
--          DROP TRIGGER koknaev.medree_prdisp_bi;
--      после заливки — создать заново (скрипт 04).
--   3) Таблица должна быть ПУСТОЙ (одиночный проход не умеет докладывать):
--          TRUNCATE TABLE koknaev.medree_prdisp;
--      При обрыве — direct-path откатывается быстро (сброс HWM), просто повторить.
--   4) Запустить блок ниже. Прогресс наблюдать из ДРУГОЙ сессии:
--          SELECT * FROM koknaev.medree_prdisp_log ORDER BY ts DESC;      -- старт/финиш
--          SELECT sid, opname, target, sofar, totalwork,
--                 ROUND(sofar/NULLIF(totalwork,0)*100) pct, time_remaining
--          FROM   v$session_longops WHERE time_remaining > 0;             -- сканы
--          SELECT s.sid, s.event, s.seconds_in_wait FROM v$session s
--          WHERE  s.username = USER AND s.status = 'ACTIVE';
--
-- ВНИМАНИЕ к именам: целевая колонка называется GROUPPCODE (две P), источник —
-- spstandardgr.GROUPCODE (одна P). Прочее выверить под реальную БД:
--   koknaev.medree(m): id_rz, code_mes1, date_2, recid, mo, accno, accdt
--   koknaev.spstandardgr(r): medstandard, dbegin, dend, groupcode
--   koknaev.expmed(f): recid, stepexp, svodno
--   koknaev.medcheck: mo, accno, accdt, mekdt
--------------------------------------------------------------------------------

-- Таблица-лог прогресса (создать один раз; ORA-00955 «уже существует» глушим).
DECLARE
    e_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_exists, -955);
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

-- Запись прогресса АВТОНОМНОЙ транзакцией — видна сразу из другой сессии.
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
    v_cnt PLS_INTEGER;
BEGIN
    koknaev.medree_prdisp_log_p(NULL, NULL, NULL, 'СТАРТ одиночной заливки');

    INSERT /*+ APPEND */ INTO koknaev.medree_prdisp (idrw, id, grouppcode, year, month)
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
    WHERE  NOT EXISTS (SELECT 1 FROM koknaev.expmed f
                       WHERE f.recid = m.recid
                         AND f.stepexp = 1
                         AND f.svodno IS NOT NULL)
    AND    EXISTS     (SELECT 1 FROM koknaev.medcheck c
                       WHERE c.mo = m.mo
                         AND c.accno = m.accno
                         AND c.accdt = m.accdt
                         AND c.mekdt IS NOT NULL);

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    koknaev.medree_prdisp_log_p(NULL, v_cnt, v_cnt, 'ФИНИШ: залито строк');
    DBMS_OUTPUT.PUT_LINE('Готово. Залито строк: ' || v_cnt);
END;
/

--------------------------------------------------------------------------------
-- ЕСЛИ ОДИН ПРОХОД ВСЁ-ТАКИ НЕ УКЛАДЫВАЕТСЯ (мало TEMP / нужно дробить) —
-- дробить по ГОДАМ (не по месяцам: ~15 итераций вместо ~180) и обязательно так,
-- чтобы год ограничивал обе стороны. Пример тела цикла:
--
--   FOR y IN (SELECT DISTINCT EXTRACT(YEAR FROM date_2) yr FROM koknaev.medree
--             ORDER BY 1) LOOP
--       INSERT /*+ APPEND */ INTO koknaev.medree_prdisp (idrw, id, grouppcode, year, month)
--       SELECT ... FROM koknaev.medree m ...
--       WHERE  m.date_2 >= TO_DATE(y.yr || '-01-01','YYYY-MM-DD')
--         AND  m.date_2 <  TO_DATE((y.yr+1) || '-01-01','YYYY-MM-DD')
--         AND  ... ;
--       COMMIT;
--   END LOOP;
--
-- Радикально ускорить помесячную/погодовую нарезку помог бы индекс
--   CREATE INDEX koknaev.medree_date2_ix ON koknaev.medree (date_2);
-- тогда предикат по дате станет driving. Без него дробление только вредит.
--------------------------------------------------------------------------------
