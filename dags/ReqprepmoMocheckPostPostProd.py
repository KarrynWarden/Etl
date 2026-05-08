"""DAG: postgres -> postgres. reqprepmo -> mocheck (doctype = 7)."""
import datetime as dt

from airflow.models import DAG

from dags._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl

with DAG(
    dag_id="ReqprepmoMocheckPostPostProd",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostPost", "reqprepmo", "mocheck", "prod", "DbSync"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    # tableNameEtlJobs отличается от tableNameMaster, чтобы не конфликтовать
    # с обычным reqprepmo->oracle направлением.
    do_etl = buildOperator(
        "do_etl",
        runEtl(
            tableNameMaster="reqprepmo",
            dbMaster="Post",
            dbSlave="Post",
            tableNameEtlJobs="reqprepmomocheck",
        ),
    )
    do_etl
