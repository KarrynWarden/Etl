"""DAG: Orcl->Post для TFINSCHET."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="TfinschetOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['OrclPost', 'TFINSCHET', 'DbSync'],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_TFINSCHET",
        tableNameMaster="TFINSCHET", dbMaster="Orcl", dbSlave="Post",
        tableNameEtlJobs="TFINSCHET",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
