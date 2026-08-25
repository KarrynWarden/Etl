-- Триггер ведущей REQPREPSMO (PostgreSQL).
--
-- Пишет в журнал ДВЕ метки на каждое изменение:
--   'reqprepsmo' — своя линия, перенос REQPREPSMO в Oracle;
--   'fublin'     — ЗАВИСИМОСТЬ линии reqprepmomocheck (ключ iudDependencies в
--                  etlFolder/config.d/reqprepmomocheckPostPost.json).
--
-- Зачем вторая метка. Строка попадает в выборку reqprepmomocheck только когда
-- у связанной REQPREPSMO проставлен DIRDT — а проставляют его ТРЕТЬИМ шагом,
-- когда сама REQPREPMO уже заведена и больше не меняется. Её собственный
-- триггер в этот момент молчит совершенно по делу, и без метки перенос об
-- изменении не узнает никогда.
--
-- Триггер при этом остаётся ГЛУПЫМ: ни DIRDT, ни JOIN в нём нет. Он говорит
-- «строка с таким ключом трогалась», а что из этого следует, выясняет перенос,
-- сравнивая два списка — что запрос линии отдаёт сейчас и что связано с ключом
-- по таблице. Разница и есть строки, выпавшие из выборки; их он удаляет.
-- Подробно — README, раздел «Зависимости: iudDependencies».
--
-- ПЕРЕД ПРИМЕНЕНИЕМ — посмотреть, как называется триггер, который стоит сейчас.
-- DROP ниже сносит его ТОЛЬКО если имя совпало (tr_reqprepsmo_after_iud). Если
-- прежний назван иначе, он останется рядом, и на каждое изменение в журнал
-- поедут ДВЕ строки 'reqprepsmo' вместо одной. Перенос от этого не сломается
-- (повтор по тому же ключу идемпотентен), но журнал распухнет вдвое, а разбор
-- станет вдвое дороже — и заметить это по данным нельзя никак:
--
--   SELECT tgname, pg_get_triggerdef(oid)
--     FROM pg_trigger
--    WHERE tgrelid = 'reqprepsmo'::regclass AND NOT tgisinternal;
--
-- Лишний снести: DROP TRIGGER <имя> ON reqprepsmo;
--
-- Триггер BEFORE INSERT (tr_etl_reqprepsmo_before_i_func — createdate,
-- lastupdate, nextval для idrw) этот скрипт НЕ трогает: он про другое и должен
-- остаться. Порядок сохраняется — BEFORE отрабатывает раньше AFTER, поэтому
-- здесь createdate уже заполнен.
--
-- Отличия от прежней версии функции, кроме второй метки:
--   RETURN NULL вместо RETURN old — для AFTER-триггера значение игнорируется,
--   но old на INSERT не присвоен, и обращаться к нему нельзя;
--   на INSERT больше не делается `old.createdate := clock_timestamp()` — в
--   AFTER INSERT такого old не существует.
--
-- Сгенерирован конструктором (вкладка «Триггеры»); правки руками при
-- пересборке будут перезаписаны.

-- Триггер IUD для линий reqprepsmo, fublin (Post).
-- Ведущая: reqprepsmo; журнал: etl_user.etl_log_iud_row.
-- Одна ведущая — несколько линий: reqprepsmo (период createdate), fublin (период createdate).
-- Триггер пишет в журнал по строке НА КАЖДУЮ линию, одинаково и
-- безусловно: разбирать, какая строка к какой линии относится, он
-- не должен и не умеет. Лишняя запись стоит одной проверки среза
-- у чужой линии и ничего не портит; отсутствующая стоила бы
-- потерянного изменения.
-- Сгенерирован конструктором (tools/trigger_builder.py); правки
-- руками при пересборке линии будут перезаписаны.
CREATE OR REPLACE FUNCTION tr_etl_reqprepsmo_after_iud_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    p_id      text;
    p_oper    varchar(2);
    p_period1 date;
    p_period2 date;
BEGIN
    IF TG_OP = 'DELETE' THEN
        p_id      := old.idrw::text;
        p_oper    := 'D';
        p_period1 := old.createdate;
        p_period2 := old.createdate;
    ELSE
        p_id      := new.idrw::text;
        p_oper    := 'IU';
        p_period1 := new.createdate;
        p_period2 := new.createdate;
        -- reqprepsmo: сменился PK или период — старую строку ведомой нужно удалить.
        IF TG_OP = 'UPDATE'
           AND (old.idrw::text <> new.idrw::text
                OR coalesce(old.createdate, '1900-01-01'::date)
                <> coalesce(new.createdate, '1900-01-01'::date)) THEN
            INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('reqprepsmo', clock_timestamp(), 'D', old.createdate, old.idrw::text, 0);
        END IF;
        -- fublin: сменился PK или период — старую строку ведомой нужно удалить.
        IF TG_OP = 'UPDATE'
           AND (old.idrw::text <> new.idrw::text
                OR coalesce(old.createdate, '1900-01-01'::date)
                <> coalesce(new.createdate, '1900-01-01'::date)) THEN
            INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
            VALUES ('fublin', clock_timestamp(), 'D', old.createdate, old.idrw::text, 0);
        END IF;
    END IF;

    INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('reqprepsmo', clock_timestamp(), p_oper, p_period1, p_id, 0);
    INSERT INTO etl_user.etl_log_iud_row(tablename, timeoper, oper, period, id, isetl)
    VALUES ('fublin', clock_timestamp(), p_oper, p_period2, p_id, 0);
    RETURN NULL;
END;
$function$;

DROP TRIGGER IF EXISTS tr_reqprepsmo_after_iud ON reqprepsmo;

CREATE TRIGGER tr_reqprepsmo_after_iud
AFTER INSERT OR UPDATE OR DELETE ON reqprepsmo
FOR EACH ROW EXECUTE PROCEDURE tr_etl_reqprepsmo_after_iud_func();

