"""DAG: Post→Orcl для idead (KOKNAEV.IDEAD)."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="IdeadPostOrcl",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostOrcl", "idead", "DbSync", "A56"],
    schedule_interval='50 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *',
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_idead",
        tableNameMaster="idead", dbMaster="Post", dbSlave="Orcl",
        retryMode="rare",
    )
    addFreezeWatcher([task], retryMode="rare")
