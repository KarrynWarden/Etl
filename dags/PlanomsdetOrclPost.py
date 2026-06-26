"""DAG: Orcl→Post для PLANOMSDET (tpplanomsdet)."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="PlanomsdetOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "planomsdet", "DbSync", "A58"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_planomsdet",
        tableNameMaster="PLANOMSDET", dbMaster="Orcl", dbSlave="Post",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
