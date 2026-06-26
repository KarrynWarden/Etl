"""DAG: Post→Orcl для pddir (KOKNAEV.PDDIR)."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="PddirPostOrcl",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostOrcl", "pddir", "DbSync", "A55"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_pddir",
        tableNameMaster="pddir", dbMaster="Post", dbSlave="Orcl",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
