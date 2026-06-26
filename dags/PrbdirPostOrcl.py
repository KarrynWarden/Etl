"""DAG: Post→Orcl для prbdir (KOKNAEV.PRBDIR)."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="PrbdirPostOrcl",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostOrcl", "prbdir", "DbSync", "A55"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_prbdir",
        tableNameMaster="prbdir", dbMaster="Post", dbSlave="Orcl",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
