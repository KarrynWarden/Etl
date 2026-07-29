"""DAG: oracle -> postgres для mocheck.

mocheck на postgres — сводная таблица с 9 типами doctype (1..9). Этот даг
переносит в неё данные из Oracle: MEDCHECK (1), EXPMED (2,3,4), PODCHECK и
SPPODNORM (5,6), TFOUTSCHET (8), TFINSCHET (9). doctype=7 сюда не входит —
он приезжает из Postgres (см. даг ReqprepmoMocheckPostPost).

Источник для всех 9 — общий MOCHECK.sql (UNION ALL, в каждой ветке свой
doctype). Логические группы различаются filterClause/filterClauseSlave
по doctype и собственным tableNameEtlJobs (MEDCHECK, EXPMED, PODCHECK,
TFOUTSCHET, TFINSCHET) для отслеживания в etl_jobs.

Режимы заданы в config.d по линиям, а не здесь:
  * EXPMED (2,3,4) и PODCHECK (5,6) — delete_insert: строка источника
    переезжает между doctype, поэтому на событие по idrw удаляются ВСЕ его
    строки в пределах своего среза и заливается то, что ведущая отдаёт
    сейчас. Иначе в mocheck остаётся осиротевшая строка в прежнем doctype;
  * остальные — iud (точечный upsert по журналу).
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import (DEFAULT_ARGS, buildOperator, configureLogger, runEtl,
                                   makeEtlOperator, addFreezeWatcher, lineEnabled)


# (tableNameEtlJobs, doctype) — порядок соответствует CASE-выражению в SQL.
# EXPMED и PODCHECK — по одной линии на несколько doctype: их строки переезжают
# между doctype (expmed 2<->3<->4, podcheck 5<->6 при смене typehelp), и линия,
# видящая только свой doctype, оставляет в mocheck осиротевшую строку в чужом.
# Обе идут в режиме delete_insert.
MOCHECK_LOGICAL_GROUPS = [
    ("MEDCHECK", 1),
    ("EXPMED", "2,3,4"),
    ("PODCHECK", "5,6"),
    # doctype=7 здесь НЕ переносится: у REQPREPMO ведущей стала сторона
    # Postgres, и mocheck doctype=7 наполняет линия reqprepmomocheckPostPost
    # (даг ReqprepmoMocheckPostPost). Линия REQPREPMOOrclPost — в архиве
    # (disabled + skipAudit), её конфиг оставлен на случай возврата.
    #("REQPREPMO", 7),
    ("TFOUTSCHET", 8),
    ("TFINSCHET", 9),
]


with DAG(
    dag_id="MocheckOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "mocheck", "DbSync", "section"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()

    previous = None
    # Линии, убранные из работы через конструктор (флаг `disabled` в конфиге),
    # пропускаем: своего файла дага у них нет, поэтому «архив» = этот флаг.
    tasks = [
        makeEtlOperator(
            f"do_etl_{group}",
            tableNameMaster=group, dbMaster="Orcl", dbSlave="Post",
            tableNameEtlJobs=group, retryMode="frequent",
        )
        for group, _doctype in MOCHECK_LOGICAL_GROUPS
        if lineEnabled(group, "Orcl", "Post")
    ]
    if tasks:
        addFreezeWatcher(tasks, retryMode="frequent")
    #addPauseWatcher(tasks)
    #for groupName, _doctype in MOCHECK_LOGICAL_GROUPS:
    #    task = buildOperator(
    #        f"do_etl_{groupName}",
    #        runEtl(
    #            tableNameMaster=groupName,
    #            dbMaster="Orcl",
    #            dbSlave="Post",
    #            tableNameEtlJobs=groupName,
    #        ),
    #        triggerRule ="all_done"
    #    )
        # Последовательно — чтобы не перегружать соединения и предсказуемо
        # обновлять mocheck по одному doctype за раз.
        #if previous is not None:
        #    previous >> task
        #previous = task
