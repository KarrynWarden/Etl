--------------------------------------------------------------------------------
-- Medree_prdisp, ЧАСТЬ 1: ежедневный ночной пересчёт ВНУТРИ Oracle
--                          + постановка isokaudit = 4 в ETL_JOBS.
--
-- Процесс из двух частей, стык между ними — таблица ETL_JOBS:
--
--   ЧАСТЬ 1 (этот файл, Oracle, 02:00)
--       гейт по medcheck.mekdt за вчера -> пересчёт окна последних 18 месяцев
--       в koknaev.medree_prdisp -> для КАЖДОГО реально изменившегося периода
--       (year, month) ставится ETL_JOBS.ISOKAUDIT = 4.
--
--   ЧАСТЬ 2 (Airflow-даг MedreeprdispOrclPost, режим ETL `section`, 05:00 и 07:00)
--       берёт из ETL_JOBS группы своей линии с ISOKAUDIT = 4 и перезаливает
--       каждую в Postgres целиком: DELETE группы в ведомой + заливка из ведущей,
--       затем ISOKAUDIT = 0 и запись в ETL_LOG.
--
-- Контракт стыка (нарушишь — часть 2 просто ничего не увидит):
--   ETL_JOBS.TABLENAME = 'MEDREE_PRDISP'  — РОВНО так, ВЕРХНИЙ регистр
--       (сравнение в SQL части 2 регистрозависимое; значение = tableNameEtlJobs
--        из dags/MedreeprdispOrclPost.py);
--   ETL_JOBS.PERIOD    = ПЕРВОЕ ЧИСЛО МЕСЯЦА, тип DATE, время 00:00
--       (часть 2 собирает период из year/month тем же TO_DATE 'ГГГГ-ММ-01');
--   ETL_JOBS.ISOKAUDIT = 4;
--   строки ETL_JOBS может и не быть — mark-процедура ниже её создаст (MERGE).
--
-- ПОЧЕМУ ПОМЕЧАЕМ НЕ ВСЁ ОКНО, А ТОЛЬКО ИЗМЕНИВШИЕСЯ ПЕРИОДЫ. Постановка 821
-- говорит «удалить данные за последние 18 месяцев и добавить заново». Если так
-- же слепо пометить все 18 периодов, часть 2 будет КАЖДУЮ ночь перезаливать в
-- Postgres ~1.6 млн строк (18 месяцев по ~90 тыс.), хотя меняются обычно
-- один-два свежих месяца. Поэтому эталон окна сначала собирается в staging
-- (medree_prdisp_stg), сравнивается с текущим содержимым medree_prdisp, и
-- дальше и перезапись в Oracle, и пометка isokaudit=4 делаются ТОЛЬКО по
-- периодам с реальными отличиями. Итог для таблицы тот же, что при слепом
-- «удалить и залить», трафик на Postgres — на порядки меньше. Нужно старое
-- поведение (например, после ручной правки данных) — вызвать с p_force => 1.
--
-- ЗАПУСКАТЬ под ВЛАДЕЛЬЦЕМ таблиц (koknaev). Схемы указаны явно: процедура
-- работает с правами определившего (AUTHID DEFINER — умолчание), а роли внутри
-- PL/SQL НЕ действуют. Если владелец процедуры не владеет какой-то из таблиц,
-- ему нужны ПРЯМЫЕ гранты (GRANT SELECT ON ... TO koknaev), иначе ORA-00942
-- при компиляции.
--
-- ЗАВИСИМОСТИ: 04_medree_prdisp_idrw.sql (sequence medree_prdisp_seq),
-- 05_medree_prdisp_initial_fill.sql (таблица-лог medree_prdisp_log и процедура
-- medree_prdisp_log_p).
--
-- ВЫВЕРИТЬ имена/схемы под реальную БД (см. также 05_*): medree(id_rz, code_mes1,
-- date_2, recid, mo, accno, accdt), spstandardgr(medstandard, dbegin, dend,
-- groupcode), expmed(recid, stepexp, svodno), medcheck(mo, accno, accdt, mekdt),
-- etl_jobs(tablename, period, last_success_ts, isokaudit).
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- 0. ПРЕДПОСЫЛКА: индекс по периоду.
--
-- Ежедневный пересчёт удаляет данные месяца (WHERE year = ? AND month = ?) и
-- сканирует окно последних 18 месяцев. Существующий ix_medree_prdisp (id, year,
-- groupcode) для этого бесполезен — без индекса ниже каждый DELETE будет полным
-- сканом 16-миллионной таблицы, и ночной джоб не уложится.
--------------------------------------------------------------------------------
CREATE INDEX koknaev.medree_prdisp_ym_ix ON koknaev.medree_prdisp (year, month);


--------------------------------------------------------------------------------
-- 1. Staging: эталон окна («как должно быть») на время одного прогона.
--
-- Обычная таблица, а не GTT — содержимое видно из соседней сессии, поэтому
-- разбор «почему пометился/не пометился период» делается обычным SELECT'ом.
-- Живёт от прогона до прогона (TRUNCATE в начале следующего).
--------------------------------------------------------------------------------
CREATE TABLE koknaev.medree_prdisp_stg (
    year       NUMBER,
    month      NUMBER,
    id         NUMBER,
    grouppcode NUMBER
);

CREATE INDEX koknaev.medree_prdisp_stg_ym_ix
    ON koknaev.medree_prdisp_stg (year, month);


--------------------------------------------------------------------------------
-- 2. Пометка периода для части 2: ETL_JOBS.ISOKAUDIT = 4.
--
-- Отдельная процедура, потому что метка нужна из трёх мест: из ночного джоба,
-- из разового bootstrap-блока (раздел 5) и руками, когда период надо перегнать
-- в Postgres принудительно:
--     BEGIN koknaev.medree_prdisp_mark(2026, 7); COMMIT; END;
--
-- COMMIT внутри НЕТ намеренно: вызывающий коммитит метку в одной транзакции с
-- самими данными периода. Иначе часть 2 может увидеть isokaudit=4 раньше, чем
-- зафиксирован пересчёт, и перенесёт в Postgres старое содержимое.
--
-- idrw в INSERT не передаётся — в etl_jobs он заполняется своим механизмом
-- (так же, как в штатном RegisterPeriodOrcl.sql самого ETL).
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE koknaev.medree_prdisp_mark(
    p_year      IN NUMBER,
    p_month     IN NUMBER,
    p_tablename IN VARCHAR2 DEFAULT 'MEDREE_PRDISP'
) AS
    v_period DATE;
BEGIN
    IF p_year IS NULL OR p_month IS NULL THEN
        -- Период без года/месяца часть 2 адресовать не умеет (группа задаётся
        -- парой year+month). Такие строки — сигнал о мусоре в medree_prdisp,
        -- см. диагностику в разделе 6.
        RETURN;
    END IF;

    v_period := TO_DATE(TO_CHAR(p_year) || '-' ||
                        LPAD(TO_CHAR(p_month), 2, '0') || '-01', 'YYYY-MM-DD');

    MERGE INTO koknaev.etl_jobs t
    USING (SELECT p_tablename AS tablename, v_period AS period FROM dual) s
       ON (t.tablename = s.tablename AND t.period = s.period)
    WHEN MATCHED THEN
        UPDATE SET t.isokaudit = 4
    WHEN NOT MATCHED THEN
        INSERT (tablename, period, last_success_ts, isokaudit)
        VALUES (s.tablename, s.period, NULL, 4);
END;
/


--------------------------------------------------------------------------------
-- 3. Ежедневный пересчёт.
--
-- p_months — глубина окна в месяцах, считая текущий (18 = постановка 821).
--            Уменьшить, если ночной прогон не укладывается по времени.
-- p_force  — 1: пометить isokaudit=4 для ВСЕХ периодов окна, даже если данные
--            не изменились (полная сверка Oracle -> Postgres «в лоб»).
--
-- Шаги: гейт -> эталон окна в staging -> периоды с отличиями -> по каждому
-- DELETE+INSERT в medree_prdisp и метка isokaudit=4, COMMIT на период.
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE koknaev.medree_prdisp_refresh(
    p_months IN PLS_INTEGER DEFAULT 18,
    p_force  IN PLS_INTEGER DEFAULT 0
) AS
    TYPE t_period  IS RECORD (year NUMBER, month NUMBER);
    TYPE t_periods IS TABLE OF t_period;

    v_periods t_periods;
    v_from    DATE        := ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -(p_months - 1));
    v_year    NUMBER;
    v_month   NUMBER;
    v_gate    PLS_INTEGER;
    v_stg     PLS_INTEGER;
    v_del     PLS_INTEGER;
    v_ins     PLS_INTEGER;
    v_total   PLS_INTEGER := 0;
BEGIN
    v_year  := EXTRACT(YEAR  FROM v_from);
    v_month := EXTRACT(MONTH FROM v_from);

    -- 3.1. Гейт: есть ли medcheck.mekdt за вчера. Нет — ночью считать нечего.
    SELECT COUNT(*) INTO v_gate FROM dual
    WHERE EXISTS (SELECT 1
                  FROM   koknaev.medcheck c
                  WHERE  c.mekdt >= TRUNC(SYSDATE) - 1
                    AND  c.mekdt <  TRUNC(SYSDATE));

    IF v_gate = 0 THEN
        koknaev.medree_prdisp_log_p(NULL, 0, 0,
            'Пропуск: нет medcheck.mekdt за вчера');
        RETURN;
    END IF;

    koknaev.medree_prdisp_log_p(TO_CHAR(v_from, 'YYYY-MM'), NULL, NULL,
        'СТАРТ окна ' || p_months || ' мес.' ||
        CASE WHEN p_force = 1 THEN ' (force)' ELSE '' END);

    -- 3.2. Эталон окна («как должно быть») — тот же запрос, что в первичном
    --      заполнении 05_*, но ограниченный по date_2 снизу. Верхней границы
    --      НЕТ намеренно: окно на обеих сторонах сравнения (staging и таблица)
    --      определяется одинаково «от v_from и дальше», иначе строки будущими
    --      датами попадали бы в таблицу, но не в эталон, и каждую ночь
    --      считались бы изменением.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE koknaev.medree_prdisp_stg';

    INSERT /*+ APPEND */ INTO koknaev.medree_prdisp_stg (year, month, id, grouppcode)
    SELECT EXTRACT(YEAR  FROM m.date_2),
           EXTRACT(MONTH FROM m.date_2),
           m.id_rz,
           r.groupcode
    FROM   koknaev.medree m
    JOIN   koknaev.spstandardgr r
           ON  m.code_mes1 = r.medstandard
           AND m.date_2 BETWEEN r.dbegin AND r.dend
           AND r.groupcode IN (1, 2, 3)
    WHERE  m.date_2 >= v_from
    AND    NOT EXISTS (SELECT 1 FROM koknaev.expmed f
                       WHERE f.recid = m.recid
                         AND f.stepexp = 1
                         AND f.svodno IS NOT NULL)
    AND    EXISTS     (SELECT 1 FROM koknaev.medcheck c
                       WHERE c.mo = m.mo
                         AND c.accno = m.accno
                         AND c.accdt = m.accdt
                         AND c.mekdt IS NOT NULL);
    v_stg := SQL%ROWCOUNT;
    -- APPEND (direct-path) держит таблицу заблокированной до конца транзакции:
    -- без COMMIT следующее же чтение staging упало бы с ORA-12838.
    COMMIT;

    koknaev.medree_prdisp_log_p(TO_CHAR(v_from, 'YYYY-MM'), v_stg, v_stg,
        'staging собран');

    -- 3.3. Периоды окна, где содержимое отличается от эталона.
    --      Сравниваем МУЛЬТИМНОЖЕСТВА: (id, grouppcode) + COUNT(*) внутри
    --      месяца, т.к. одна пара (ЗЛ, группа) в месяце может законно
    --      повторяться — так заливало первичное заполнение 05_*, по строке на
    --      запись medree. MINUS в обе стороны ловит и лишние, и пропавшие
    --      строки; UNION по периодам сводит всё к списку (year, month).
    --      BULK COLLECT (а не курсорный FOR) — потому что ниже в цикле идут
    --      COMMIT'ы: открытый курсор поверх тяжёлого MINUS словил бы ORA-01555.
    IF p_force = 1 THEN
        SELECT DISTINCT year, month
        BULK COLLECT INTO v_periods
        FROM  (SELECT year, month FROM koknaev.medree_prdisp_stg
               UNION
               SELECT year, month FROM koknaev.medree_prdisp
               WHERE  year >= v_year AND (year > v_year OR month >= v_month))
        WHERE year IS NOT NULL AND month IS NOT NULL;
    ELSE
        SELECT DISTINCT year, month
        BULK COLLECT INTO v_periods
        FROM (
            SELECT year, month FROM (
                SELECT year, month, id, grouppcode, COUNT(*) AS cnt
                FROM   koknaev.medree_prdisp_stg
                GROUP  BY year, month, id, grouppcode
                MINUS
                SELECT year, month, id, grouppcode, COUNT(*)
                FROM   koknaev.medree_prdisp
                WHERE  year >= v_year AND (year > v_year OR month >= v_month)
                GROUP  BY year, month, id, grouppcode
            )
            UNION
            SELECT year, month FROM (
                SELECT year, month, id, grouppcode, COUNT(*) AS cnt
                FROM   koknaev.medree_prdisp
                WHERE  year >= v_year AND (year > v_year OR month >= v_month)
                GROUP  BY year, month, id, grouppcode
                MINUS
                SELECT year, month, id, grouppcode, COUNT(*)
                FROM   koknaev.medree_prdisp_stg
                GROUP  BY year, month, id, grouppcode
            )
        )
        WHERE year IS NOT NULL AND month IS NOT NULL;
    END IF;

    IF v_periods.COUNT = 0 THEN
        koknaev.medree_prdisp_log_p(TO_CHAR(v_from, 'YYYY-MM'), 0, v_stg,
            'ФИНИШ: изменившихся периодов нет');
        RETURN;
    END IF;

    -- 3.4. По каждому изменившемуся периоду: перезапись в Oracle + метка для
    --      части 2. Данные и метка коммитятся ВМЕСТЕ, поэтому ETL никогда не
    --      увидит isokaudit=4 на ещё не зафиксированном периоде.
    FOR i IN 1 .. v_periods.COUNT LOOP
        DELETE FROM koknaev.medree_prdisp
        WHERE  year = v_periods(i).year AND month = v_periods(i).month;
        v_del := SQL%ROWCOUNT;

        INSERT INTO koknaev.medree_prdisp (idrw, id, grouppcode, year, month)
        SELECT koknaev.medree_prdisp_seq.NEXTVAL, s.id, s.grouppcode, s.year, s.month
        FROM   koknaev.medree_prdisp_stg s
        WHERE  s.year = v_periods(i).year AND s.month = v_periods(i).month;
        v_ins := SQL%ROWCOUNT;

        koknaev.medree_prdisp_mark(v_periods(i).year, v_periods(i).month);
        COMMIT;

        v_total := v_total + v_ins;
        koknaev.medree_prdisp_log_p(
            TO_CHAR(v_periods(i).year) || '-' ||
            LPAD(TO_CHAR(v_periods(i).month), 2, '0'),
            v_ins, v_total,
            'период перезалит (-' || v_del || '/+' || v_ins || '), isokaudit=4');
    END LOOP;

    koknaev.medree_prdisp_log_p(TO_CHAR(v_from, 'YYYY-MM'), v_periods.COUNT, v_total,
        'ФИНИШ: периодов помечено ' || v_periods.COUNT);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        koknaev.medree_prdisp_log_p(TO_CHAR(v_from, 'YYYY-MM'), NULL, v_total,
            'ОШИБКА: ' || SUBSTR(SQLERRM, 1, 150));
        RAISE;
END;
/


--------------------------------------------------------------------------------
-- 4. Планировщик: каждый день в 02:00 — ДО части 2 (даг в 05:00 и 07:00).
--    Пересоздание — сначала снять старый job.
--------------------------------------------------------------------------------
BEGIN
    BEGIN
        DBMS_SCHEDULER.DROP_JOB('KOKNAEV.MEDREE_PRDISP_DAILY', force => TRUE);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'KOKNAEV.MEDREE_PRDISP_DAILY',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'KOKNAEV.MEDREE_PRDISP_REFRESH',
        start_date      => TRUNC(SYSDATE) + 1 + 2/24,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2; BYMINUTE=0; BYSECOND=0',
        enabled         => TRUE,
        comments        => 'Пересчёт Medree_prdisp за 18 мес. + isokaudit=4 для ETL'
    );
END;
/


--------------------------------------------------------------------------------
-- 5. РАЗОВО, ПЕРЕД ПЕРВЫМ ЗАПУСКОМ ДАГА: первичный перенос в пустой Postgres.
--
-- Часть 2 в режиме `section` переносит ТОЛЬКО то, что помечено isokaudit=4.
-- Таблица в Oracle уже заполнена скриптом 05_*, а в Postgres пусто — значит
-- надо один раз пометить ВСЕ периоды, иначе даг честно отработает вхолостую
-- («групп с isokaudit=4 нет») и Postgres так и останется пустым.
--
-- Дальше это не нужно: свежие периоды помечает ночной джоб.
--
-- ~180 периодов по ~90 тыс. строк = ~16 млн: первый прогон дага длинный (часы).
-- max_active_runs=1 не даст запуску в 07:00 наложиться на запуск в 05:00;
-- недоделанные периоды остаются с isokaudit=4 и доберутся следующими
-- запусками — прогон можно спокойно прерывать.
--------------------------------------------------------------------------------
-- BEGIN
--     FOR p IN (SELECT DISTINCT year, month
--               FROM   koknaev.medree_prdisp
--               WHERE  year IS NOT NULL AND month IS NOT NULL) LOOP
--         koknaev.medree_prdisp_mark(p.year, p.month);
--     END LOOP;
--     COMMIT;
-- END;
-- /


--------------------------------------------------------------------------------
-- 6. Наблюдение и диагностика
--------------------------------------------------------------------------------
-- Ход ночного джоба (пишется автономными транзакциями — видно сразу):
--   SELECT * FROM koknaev.medree_prdisp_log ORDER BY ts DESC;
--
-- Очередь для части 2 и что она уже перенесла:
--   SELECT period, isokaudit, last_success_ts
--   FROM   koknaev.etl_jobs
--   WHERE  tablename = 'MEDREE_PRDISP'
--   ORDER  BY period DESC;
--     isokaudit = 4  — ждёт переноса;
--               = 0  — перенесено, ждёт аудита (даг AuditAll, 18-23ч);
--               = 1  — аудит подтвердил совпадение;
--               = -1 — ошибка переноса группы (смотреть koknaev.etl_log);
--               = -4 — аудит нашёл расхождение.
--
-- Что делал ETL по этой таблице:
--   SELECT * FROM koknaev.etl_log
--   WHERE  tablename = 'MEDREE_PRDISP' ORDER BY timeoper DESC;
--
-- Строки без периода — их часть 2 адресовать не может и НИКОГДА не перенесёт
-- (группа = пара year+month). Должно быть 0; если нет — в medree попали
-- записи с date_2 IS NULL, чинить источник:
--   SELECT COUNT(*) FROM koknaev.medree_prdisp
--   WHERE  year IS NULL OR month IS NULL;
--
-- Разбор «почему период помечен»: staging хранит эталон последнего прогона.
--   SELECT COUNT(*) FROM koknaev.medree_prdisp_stg WHERE year = 2026 AND month = 7;
--   SELECT COUNT(*) FROM koknaev.medree_prdisp     WHERE year = 2026 AND month = 7;
--
-- Ручной прогон вне расписания:
--   BEGIN koknaev.medree_prdisp_refresh(); END;                  -- как ночью
--   BEGIN koknaev.medree_prdisp_refresh(p_months => 3); END;     -- только 3 мес.
--   BEGIN koknaev.medree_prdisp_refresh(p_force => 1); END;      -- пометить всё окно
--   BEGIN koknaev.medree_prdisp_mark(2026, 7); COMMIT; END;      -- один период
