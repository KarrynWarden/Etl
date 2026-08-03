"""DAG: Post->Orcl для eindexmo."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="EindexmoPostOrcl",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['PostOrcl', 'eindexmo', 'DbSync'],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_eindexmo",
        tableNameMaster="eindexmo", dbMaster="Post", dbSlave="Orcl",
        tableNameEtlJobs="eindexmo",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
