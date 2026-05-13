"""DAG: postgres -> oracle. Тот же reqprepmo, период reqdt."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl

with DAG(
    dag_id="ReqprepmoPostOrclProd",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostOrcl", "reqprepmo", "prod", "DbSync"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    do_etl = buildOperator(
        "do_etl",
        runEtl(tableNameMaster="reqprepmo", dbMaster="Post", dbSlave="Orcl"),
    )
    do_etl
