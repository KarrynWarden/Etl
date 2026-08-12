"""DAG: составной перенос — несколько линий одним дагом.

Линии перечислены в LINES: по задаче на линию, все читают свои
настройки из etlFolder/config.d. Так собирают линии одного источника
(например, все doctype mocheck из общего MOCHECK.sql), чтобы не плодить
одинаковые даги и не гонять один и тот же запрос параллельно.

Файл пишет конструктор: правки в списке линий, расписании и тегах делаются
через него (иначе следующая дописанная линия их затрёт). Свободный текст
ниже маркера — исключение: его конструктор переносит в новый файл как есть.

─── заметка (правится руками, конструктор её сохраняет) ───
mocheck на postgres — сводная таблица с 9 типами doctype (1..9). Этот даг
переносит в неё данные из Oracle:

    doctype 1     MEDCHECK
    doctype 2,3,4 EXPMED
    doctype 5,6   PODCHECK + SPPODNORM
    doctype 8     TFOUTSCHET
    doctype 9     TFINSCHET

doctype=7 сюда не входит — он приезжает из Postgres (даг
ReqprepmoMocheckPostPost): у REQPREPMO ведущей стала сторона Postgres, а линия
REQPREPMOOrclPost — в архиве (disabled + skipAudit), её конфиг оставлен на
случай возврата.

Источник для всех — общий MOCHECK.sql (UNION ALL, в каждой ветке свой
doctype). Логические группы различаются filterClause/filterClauseSlave по
doctype и собственным tableNameEtlJobs (MEDCHECK, EXPMED, PODCHECK,
TFOUTSCHET, TFINSCHET) для отслеживания в etl_jobs.

EXPMED и PODCHECK — по одной линии на НЕСКОЛЬКО doctype: их строки переезжают
между doctype (expmed 2<->3<->4, podcheck 5<->6 при смене typehelp), и линия,
видящая только свой doctype, оставляла бы в mocheck осиротевшую строку в
чужом. Поэтому обе перезаливают срез целиком, а не правят строку точечно.

Режимы переноса заданы в config.d по линиям, а не здесь.
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import (DEFAULT_ARGS, configureLogger, makeEtlOperator,
                                   addFreezeWatcher, lineEnabled)

# dagbuilder: составной даг (список линий ниже правит конструктор)
LINES = [
    ('MEDCHECK', 'Orcl', 'Post'),
    ('EXPMED', 'Orcl', 'Post'),
    ('PODCHECK', 'Orcl', 'Post'),
    ('TFOUTSCHET', 'Orcl', 'Post'),
    ('TFINSCHET', 'Orcl', 'Post'),
]

with DAG(
    dag_id="MocheckOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['OrclPost', 'mocheck', 'DbSync', 'section'],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    tasks = [
        makeEtlOperator(
            f"do_etl_{line}",
            tableNameMaster=line, dbMaster=dbm, dbSlave=dbs,
            tableNameEtlJobs=line, retryMode="frequent",
        )
        for line, dbm, dbs in LINES
        if lineEnabled(line, dbm, dbs)
    ]
    if tasks:
        addFreezeWatcher(tasks, retryMode="frequent")
