--------------------------------------------------------------------------------
-- Medree_prdisp: ЕЖЕДНЕВНЫЙ ночной инкремент (Oracle DBMS_SCHEDULER).
--
-- Логика (из ТЗ):
--   1. Гейт: если за ПРЕДЫДУЩИЙ день нет ни одной medcheck.mekdt — выходим
--      (ночью считать нечего).
--   2. Иначе берём уникальные (year, month) из medcheck с lastupdate за
--      последний месяц — это «затронутые» периоды.
--   3. Удаляем эти (year, month) из Medree_prdisp и переливаем их заново тем же
--      запросом, что и первичное заполнение (05_*), но ограниченным выбранными
--      периодами. Построчный курсор из ТЗ заменён на set-based DELETE + INSERT
--      SELECT (эквивалентно, но быстрее).
--
-- Периоды за последний месяц вынесены в единый подзапрос q_periods, чтобы DELETE
-- и INSERT видели одно и то же множество. ВЫВЕРИТЬ на тесте имена/схемы (см. 05_*),
-- а также колонки medcheck.year / medcheck.month / medcheck.lastupdate.
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE medree_prdisp_refresh AS
    v_has_yesterday NUMBER;
BEGIN
    -- 1. Гейт: есть ли medcheck.mekdt за вчера
    SELECT COUNT(*) INTO v_has_yesterday
    FROM   medcheck c
    WHERE  c.mekdt >= TRUNC(SYSDATE) - 1
      AND  c.mekdt <  TRUNC(SYSDATE);
    IF v_has_yesterday = 0 THEN
        RETURN;
    END IF;

    -- 2+3a. Удалить затронутые периоды из Medree_prdisp
    DELETE FROM Medree_prdisp p
    WHERE (p.year, p.month) IN (
        SELECT DISTINCT c.year, c.month
        FROM   medcheck c
        WHERE  c.lastupdate >= ADD_MONTHS(TRUNC(SYSDATE), -1)
    );

    -- 3b. Перелить их заново
    INSERT INTO Medree_prdisp (id, groupcode, year, month)
    SELECT m.id_rz,
           r.groupcode,
           EXTRACT(YEAR  FROM m.date_2),
           EXTRACT(MONTH FROM m.date_2)
    FROM   koknaev.medree m
    JOIN   spstandardgr r
           ON  m.code_mes1 = r.medstandard
           AND m.date_2 BETWEEN r.dbegin AND r.dend
           AND r.groupcode IN (1, 2, 3)
    WHERE  (EXTRACT(YEAR FROM m.date_2), EXTRACT(MONTH FROM m.date_2)) IN (
               SELECT DISTINCT c.year, c.month
               FROM   medcheck c
               WHERE  c.lastupdate >= ADD_MONTHS(TRUNC(SYSDATE), -1)
           )
    AND    NOT EXISTS (SELECT 1 FROM expmed f
                       WHERE f.recid = m.recid
                         AND f.stepexp = 1
                         AND f.svodno IS NOT NULL)
    AND    EXISTS     (SELECT 1 FROM medcheck c2
                       WHERE c2.mo = m.mo
                         AND c2.accno = m.accno
                         AND c2.accdt = m.accdt
                         AND c2.mekdt IS NOT NULL);

    COMMIT;
END;
/

-- Планировщик: каждый день в 02:00. Пересоздание — сначала снять старый job.
BEGIN
    BEGIN
        DBMS_SCHEDULER.DROP_JOB('MEDREE_PRDISP_DAILY', force => TRUE);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'MEDREE_PRDISP_DAILY',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'MEDREE_PRDISP_REFRESH',
        start_date      => TRUNC(SYSDATE) + 1 + 2/24,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2; BYMINUTE=0; BYSECOND=0',
        enabled         => TRUE,
        comments        => 'Ночное обновление Medree_prdisp по изменённым периодам medcheck'
    );
END;
/
