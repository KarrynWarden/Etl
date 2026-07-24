--------------------------------------------------------------------------------
-- Medree_prdisp: ПЕРВИЧНОЕ заполнение (разово, вручную).
--
-- Таблица сейчас пустая. Заливаем её целиком за все даты одним set-based
-- INSERT ... SELECT (запрос из ТЗ даёт ~16 млн строк; крупнейшие группы
-- year-month — ~400-429 тыс). Построчный курсор из ТЗ здесь заменён на
-- INSERT SELECT — логически то же самое, но на 16 млн строк несопоставимо
-- быстрее. idrw проставит триггер medree_prdisp_bi (см. 04_*).
--
-- ВНИМАНИЕ: выверить на тесте имена схем/колонок под реальную БД:
--   koknaev.medree(m): id_rz, code_mes1, date_2, recid, mo, accno, accdt
--   spstandardgr(r):   medstandard, dbegin, dend, groupcode
--   expmed(f):         recid, stepexp, svodno
--   medcheck:          mo, accno, accdt, mekdt
-- Прогонять в один заход; при желании — секционно по годам (WHERE EXTRACT(YEAR..)).
--------------------------------------------------------------------------------

INSERT INTO Medree_prdisp (id, groupcode, year, month)
SELECT m.id_rz,
       r.groupcode,
       EXTRACT(YEAR  FROM m.date_2) AS year,
       EXTRACT(MONTH FROM m.date_2) AS month
FROM   koknaev.medree m
JOIN   spstandardgr r
       ON  m.code_mes1 = r.medstandard
       AND m.date_2 BETWEEN r.dbegin AND r.dend
       AND r.groupcode IN (1, 2, 3)
WHERE  NOT EXISTS (SELECT 1 FROM expmed f
                   WHERE f.recid = m.recid
                     AND f.stepexp = 1
                     AND f.svodno IS NOT NULL)
AND    EXISTS     (SELECT 1 FROM medcheck c
                   WHERE c.mo = m.mo
                     AND c.accno = m.accno
                     AND c.accdt = m.accdt
                     AND c.mekdt IS NOT NULL);

COMMIT;
