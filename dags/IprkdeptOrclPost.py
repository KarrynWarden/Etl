"""DAG: Orcl->Post для iprkdept."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="IprkdeptOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['OrclPost', 'iprkdept', 'DbSync'],
    schedule_interval=dt.timedelta(minutes=10),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_iprkdept",
        tableNameMaster="IPRKDEPT", dbMaster="Orcl", dbSlave="Post",
        tableNameEtlJobs="iprkdept",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
