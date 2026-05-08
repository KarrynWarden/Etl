"""DAG: oracle -> postgres, режим section (медри-стиль).

Изменения отслеживаются по группе createdate целиком: при появлении группы
со статусом isokaudit=4 в etl_jobs функция полностью перезаливает её из
ведущей в ведомую. Подходит для таблиц с агрегатами / большими SELECT'ами,
где точечный апдейт неприменим.
"""
import datetime as dt

from airflow.models import DAG

from dags._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl

with DAG(
    dag_id="MedcheckOrclPostProd",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "medcheck", "prod", "DbSync"],
    schedule_interval=dt.timedelta(minutes=5),
    catchup=False,
) as dag:
    configureLogger()
    do_etl = buildOperator(
        "do_etl",
        runEtl(tableNameMaster="medcheck", dbMaster="Orcl", dbSlave="Post"),
    )
    do_etl
