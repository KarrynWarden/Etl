"""DAG: oracle -> postgres для mocheck.

mocheck на postgres — сводная таблица с 9 типами doctype (1..9), куда
переносятся данные из 6 разных Oracle-таблиц (MEDCHECK, EXPMED [3 ветки],
PODCHECK [2 ветки], REQPREPMO+REQPREPSMO, TFOUTSCHET, TFINSCHET).

Источник для всех 9 — общий MOCHECK.sql (UNION ALL, в каждой ветке свой
doctype). Логические группы различаются filterClause/filterClauseSlave
по doctype и собственным tableNameEtlJobs (MEDCHECK, EXPMED2, ...,
TFINSCHET) для отслеживания в etl_jobs.

Режим — section_compare: сравниваем уникальные (createdate, lastupdate)
на ведущей и ведомой сторонах в пределах одного doctype, плюс учитываем
сигналы из etl_log_iud_row. Каждая группа, нуждающаяся в обновлении,
полностью удаляется и перезаливается.

9 задач запускаются параллельно (без `>>`-цепочки). Параллелизм
ограничивается capacity пула "Prod" в UI airflow.
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl


# (tableNameEtlJobs, doctype) — порядок соответствует CASE-выражению в SQL.
MOCHECK_LOGICAL_GROUPS = [
    ("MEDCHECK", 1),
    ("EXPMED2", 2),
    ("EXPMED3", 3),
    ("EXPMED4", 4),
    ("PODCHECK3", 5),
    ("PODCHECK4", 6),
    ("REQPREPMO", 7),
    ("TFOUTSCHET", 8),
    ("TFINSCHET", 9),
]


with DAG(
    dag_id="MocheckOrclPostProd",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "mocheck", "prod", "DbSync", "section"],
    schedule_interval=dt.timedelta(minutes=5),
    catchup=False,
) as dag:
    configureLogger()

    # 9 задач без зависимостей — airflow запустит их параллельно (в пределах
    # capacity пула "Prod"). trigger_rule="all_done" на самом деле здесь не
    # активен (нет upstream'а), но оставлен на случай, если в будущем
    # вернётся `>>`-цепочка — тогда skip в одной не каскадит на остальных.
    for groupName, _doctype in MOCHECK_LOGICAL_GROUPS:
        buildOperator(
            f"do_etl_{groupName}",
            runEtl(
                tableNameMaster=groupName,
                dbMaster="Orcl",
                dbSlave="Post",
                tableNameEtlJobs=groupName,
            ),
            triggerRule="all_done",
        )
