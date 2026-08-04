-- Ошибка вызова pck_eindexmo.ProlongDestrEindex — в общий журнал ошибок.
--
-- idfunc = 286 и errcode = -1 — те же значения, что писала прежняя процедура
-- pck_eindexmo.scanlogprolong, чтобы существующие разборы ошибок продолжали
-- находить эти записи. Изменился только errtype: ошибку теперь регистрирует
-- даг, а не процедура, и в param_in лежат параметры вызова (mo/otdel/dept/
-- subdept/destrdate), а не PG_EXCEPTION_DETAIL.
INSERT INTO public.ias_func_errlog (idfunc, errtype, errcode, errtext,
                                    param_in, stacktrace)
VALUES (286, %s, -1, %s, %s, %s)
