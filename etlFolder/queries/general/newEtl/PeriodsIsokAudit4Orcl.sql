-- Группы, помеченные на полную перезаливку. NULL-период отсюда больше НЕ
-- исключается: это такая же группа, и раньше пометка isokaudit=4 на ней просто
-- ничего не делала.
SELECT period FROM koknaev.etl_jobs
WHERE tablename = :tablename AND isokaudit = 4
