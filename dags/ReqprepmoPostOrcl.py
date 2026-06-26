"""DAG: postgres -> oracle. Тот же reqprepmo, период reqdt."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl, makeEtlOperator, addFreezeWatcher

with DAG(dag_id="ReqprepmoPostOrcl", default_args=DEFAULT_ARGS,
         max_active_runs=1, schedule_interval=dt.timedelta(minutes=1),
         catchup=False, tags=["PostOrcl", "reqprepmo", "DbSync"]) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_reqprepmo",
        tableNameMaster="reqprepmo", dbMaster="Post", dbSlave="Orcl",
        retryMode="frequent",
    )
    addFreezeWatcher([task], retryMode="frequent")
    #addPauseWatcher([task])