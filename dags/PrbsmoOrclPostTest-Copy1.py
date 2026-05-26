"""DAG: oracle -> postgres. Простая таблица с master-запросом."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl

with DAG(
    dag_id="PrbsmoOrclPostTest",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "prbsmo", "test", "DbSync"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    do_etl = buildOperator(
        "do_etl",
        runEtl(tableNameMaster="PRBSMO", dbMaster="Orcl", dbSlave="Post"),
    )
    do_etl
