"""DAG: Orcl→Post для MEDREE_PRDISP (режим query_section).

Перенос Medree_prdisp Oracle→Postgres: группы (year, month) из periodsSql,
каждая группа обновляется полной перезаписью. Идёт ночью ПОСЛЕ Oracle-джоба
medree_prdisp_refresh (02:00), поэтому в 03:00 — переносим уже пересчитанные
группы. retryMode='rare' (редкий ночной запуск, ретраи делает airflow).
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="MedreeprdispOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "medree_prdisp", "DbSync"],
    schedule_interval="0 3 * * *",
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_medree_prdisp",
        tableNameMaster="KOKNAEV.MEDREE_PRDISP", dbMaster="Orcl", dbSlave="Post",
        tableNameEtlJobs="MEDREE_PRDISP",
        retryMode="rare",
    )
    addFreezeWatcher([task], retryMode="rare")
