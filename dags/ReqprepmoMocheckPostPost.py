"""DAG: postgres -> postgres. reqprepmo -> mocheck (doctype = 7)."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="ReqprepmoMocheckPostPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["PostPost", "reqprepmo", "mocheck", "DbSync"],
    schedule_interval=dt.timedelta(minutes=1),
    catchup=False,
) as dag:
    configureLogger()
    # tableNameEtlJobs отличается от tableNameMaster, чтобы не конфликтовать
    # с обычным reqprepmo->oracle направлением.
    task = makeEtlOperator(
        "do_etl_reqprepmo",
        tableNameMaster="reqprepmo", dbMaster="Post", dbSlave="Post",
        retryMode="frequent",
        tableNameEtlJobs="reqprepmomocheck",
    )
    addFreezeWatcher([task], retryMode="frequent")
    #do_etl = buildOperator(
    #    "do_etl",
    #    runEtl(
    #        tableNameMaster="reqprepmo",
    #        dbMaster="Post",
    #        dbSlave="Post",
    #        tableNameEtlJobs="reqprepmomocheck",
    #    ),
    #)
    #do_etl
