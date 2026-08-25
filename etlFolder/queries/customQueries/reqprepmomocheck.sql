-- reqprepmo -> mocheck (doctype = 7).
--
-- c.REQPREPSMO_idrw в списке колонок — НЕ данные, а точка опоры зависимости.
-- В структуру ведущей (structures/reqprepmo/reqprepmomocheck.json) эта колонка
-- не входит и в ведомую не поедет: перенос выбирает из этого запроса только то,
-- что перечислено в структуре. Но линия по ней раскладывает журнальную метку
-- 'fublin' в ключи ведущей — а разложить может только то, что запрос отдаёт.
--
-- Зачем зависимость. Строка появляется в выборке в три шага: заводится
-- REQPREPSMO с пустым DIRDT, к ней привязывается REQPREPMO, и лишь потом
-- DIRDT проставляют. На третьем шаге REQPREPMO не меняется вовсе — её триггер
-- молчит совершенно по делу, а строка в выборке уже есть. Триггер REQPREPSMO
-- пишет метку, линия её раскладывает; см. iudDependencies в конфиге линии.
SELECT mo, mo modoc, 7::numeric doctype, ACCNO, ACCDT, 0::numeric STEPEXP, NULL::numeric schetnrec, 2::numeric TYPECONT, c.smo CONT, sumprep sumcheck, 0::numeric SUMKSS, 0::numeric SUMSZP, 0::numeric SUMAPP, 0::numeric SUMSMP, 0::numeric COUNTCHECK, c.YEAR, c.MONTH, c.idrw, c.LASTUPDATE, c.createdate, null::date SVODDT, null::numeric MEDCHECK_IDRW, null::date buhdt, NULL::numeric SCHETTYPE, c.REQPREPSMO_idrw
    FROM REQPREPMO c
    INNER JOIN REQPREPSMO s ON s.IDrw = c.REQPREPSMO_idrw
    WHERE ACCDT  >= TO_DATE('2023-01-01', 'YYYY-MM-DD') AND s.DIRDT IS NOT NULL
