--------------------------------------------------------------------------------
-- Medree_prdisp: сама таблица и суррогатный ключ idrw (Oracle).
--
-- ПЕРВЫЙ скрипт этой линии — запускать до 05 (первичное заполнение) и до 06
-- (ежедневный пересчёт). Оба они таблицу уже предполагают существующей.
--
-- Состав по постановке 821:
--     IDRW        N 14   идентификатор записи (суррогатный PK, наш; в постановке
--                        он есть, но заполнение не описано — им занимается
--                        последовательность и триггер ниже)
--     ID          N 10   идентификатор ЗЛ (IPerson.ID)
--     GROUPPCODE  N 1    1 дисп. / 2 проф. / 3 центр здоровья
--     YEAR        N 4
--     MONTH       N 4
--
-- ТОЧНОСТЬ ОБЯЗАТЕЛЬНА. Голый NUMBER даёт DATA_SCALE = NULL, а json-эталон
-- (structures/MEDREE_PRDISP/MEDREE_PRDISP.json) ждёт 0 — сверка структур
-- не сошлась бы и линия встала бы с FLK. NUMBER(n) даёт scale 0.
--
-- MONTH оставлен NULLABLE намеренно. Все пути заполнения (05 и 06) кладут
-- EXTRACT(MONTH FROM m.date_2) и NULL там появиться не должен, но если он
-- всё же появится, это не должно ронять пересчёт: medree_prdisp_mark такую
-- строку ЛОВИТ и пишет в лог как мусор (см. 06, шаг 2). Ограничение NOT NULL
-- превратило бы диагностируемую аномалию в аварию посреди ночного джоба.
--
-- idrw — первичный ключ. При переносе в Postgres значение idrw переносится КАК
-- ЕСТЬ, поэтому в Postgres колонка idrw должна допускать явную вставку значения
-- (см. postgresSetup/01_medree_prdisp_slave.sql).
--------------------------------------------------------------------------------

-- Запускать под ВЛАДЕЛЬЦЕМ таблиц (koknaev) — схема указана явно, чтобы объекты
-- не искались в схеме текущего пользователя (иначе ORA-00942).

-- 1. Таблица. ORA-00955 («объект уже существует») глушится, чтобы скрипт можно
--    было прогнать повторно, не разбирая, что из него уже выполнено.
DECLARE
    e_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_exists, -955);
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE koknaev.medree_prdisp (
            idrw       NUMBER(14) NOT NULL,
            id         NUMBER(10) NOT NULL,
            grouppcode NUMBER(1)  NOT NULL,
            year       NUMBER(4)  NOT NULL,
            month      NUMBER(4),
            CONSTRAINT medree_prdisp_pk PRIMARY KEY (idrw)
        )';
EXCEPTION WHEN e_exists THEN NULL;
END;
/

-- 2. Последовательность под idrw.
--    CACHE ускоряет массовую вставку (первичное заполнение ~16 млн строк).
--    Пропуски в idrw допустимы — это суррогат, не смысловой номер.
DECLARE
    e_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_exists, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE SEQUENCE koknaev.medree_prdisp_seq '
                      || 'START WITH 1 INCREMENT BY 1 CACHE 1000';
EXCEPTION WHEN e_exists THEN NULL;
END;
/

-- 3. Автозаполнение idrw, когда его не передали явно. Образец —
--    01_create_etl_log_iud_row.sql.
CREATE OR REPLACE TRIGGER koknaev.medree_prdisp_bi
BEFORE INSERT ON koknaev.medree_prdisp
FOR EACH ROW
WHEN (NEW.idrw IS NULL)
BEGIN
    SELECT koknaev.medree_prdisp_seq.NEXTVAL INTO :NEW.idrw FROM DUAL;
END;
/

-- ВАЖНО (11g): если первичное заполнение (05_*) уже проставило idrw из sequence,
-- то после создания триггера последовательность продолжит с того же места —
-- пересинхронизация не нужна. Но если idrw когда-то вставлялись мимо sequence,
-- сверь: SELECT MAX(idrw) FROM koknaev.medree_prdisp; и текущее значение
-- koknaev.medree_prdisp_seq.CURRVAL — при отставании пересоздай sequence со
-- START WITH (MAX(idrw)+1), иначе будет ORA-00001 по PK.
