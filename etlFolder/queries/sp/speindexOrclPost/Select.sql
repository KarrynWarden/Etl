-- Источник разового переноса SPEINDEX: ведущая на Oracle.
--
-- Имя без схемы — так же, как работало до разделения файлов (etl_user видит
-- таблицу через синоним; на проде этот SELECT отрабатывал, падение было уже
-- на вставке).
--
-- Порядок и состав колонок обязаны совпадать с Add.sql — execute_values
-- подставляет значения позиционно. filedname не выбирается (см. Add.sql).
SELECT code, name, eindexform, typeindex, intonly, ei, period,
       intotemo, kosgu, fsum, formula, numbord, signext
  FROM speindex
