"""DAG: Post→Orcl для prbsmo (KOKNAEV.PRBSMO)."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="PrbsmoPostOrcl",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostOrcl", "prbsmo", "DbSync", "A83"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_prbsmo",
        tableNameMaster="prbsmo", dbMaster="Post", dbSlave="Orcl",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
