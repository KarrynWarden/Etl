"""DAG: oracle -> postgres. Простая таблица с master-запросом."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="PlanomsOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "planoms", "DbSync"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_PLANOMS",
        tableNameMaster="PLANOMS", dbMaster="Orcl", dbSlave="Post",
        retryMode="frequent"
    )
    addFreezeWatcher([task], retryMode="frequent")
