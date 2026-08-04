-- Необработанные заявки на продление ЭЦП из журнала Oracle.
--
-- Дословный аналог курсора из pck_eindexmo.scanlogprolong (версия, которая
-- жила на Postgres): по каждому ключу МО+отдел+подразделение+подподразделение
-- берётся ОДНА запись — самая свежая по timeoper среди ещё не отработанных
-- (dwork is null). Более старые заявки того же ключа закрываются вместе с ней
-- (см. MarkProcessedOrcl.sql), поэтому обрабатывать их отдельно не нужно.
--
-- Порядок по timeoper: заявки применяются в том же порядке, в каком их завели.
SELECT DISTINCT s.mo, s.otdel, s.dept, s.subdept, s.destrdate, s.timeoper
  FROM log_eindprolong s
 WHERE s.dwork IS NULL
   AND s.timeoper = (SELECT MAX(timeoper)
                       FROM log_eindprolong
                      WHERE dwork IS NULL
                        AND mo = s.mo
                        AND otdel = s.otdel
                        AND dept = s.dept
                        AND subdept = s.subdept)
 ORDER BY s.timeoper
