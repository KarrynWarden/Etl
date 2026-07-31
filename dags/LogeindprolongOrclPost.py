"""DAG: Orcl->Post для LOG_EINDPROLONG."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="LogeindprolongOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "log_eindprolong", "DbSync", "A61"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_LOG_EINDPROLONG",
        tableNameMaster="LOG_EINDPROLONG", dbMaster="Orcl", dbSlave="Post",
        tableNameEtlJobs="LOG_EINDPROLONG",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
