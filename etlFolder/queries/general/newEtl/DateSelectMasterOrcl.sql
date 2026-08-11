-- Срез ведущей: по одной строке на пару (период, lastupdate) + сколько записей
-- в этой паре. Счётчик нужен, чтобы ловить расхождения, которые не двигают
-- lastupdate: удалили строку, не державшую максимум, — множества совпадают, а
-- данные разные. GROUP BY, а не DISTINCT + COUNT(*) OVER: оконная функция
-- считается ДО DISTINCT, и каждая строка несла бы полный счёт периода, который
-- потом ещё и складывался бы по числу различных lastupdate.
SELECT {period_expr} AS createdate, lastupdate AS lastupdate, COUNT(*) AS etl_cnt
FROM ( {select_sql} ) p
GROUP BY {period_expr}, lastupdate
