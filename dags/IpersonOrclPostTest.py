"""DAG: oracle -> postgres. Простая таблица с master-запросом."""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl, makeEtlOperator, addPauseWatcher

#with DAG(
#    dag_id="IpersonOrclPostTest",
#    default_args=DEFAULT_ARGS,
#    max_active_runs=1,
#    tags=["OrclPost", "iperson", "test", "DbSync"],
#    schedule_interval=dt.timedelta(minutes=1),
#    catchup=False,
#) as dag:
#    configureLogger()
#    do_etl = buildOperator(
#        "do_etl",
#        runEtl(tableNameMaster="iperson", dbMaster="Orcl", dbSlave="Post"),
#    )
#    do_etl


with DAG(
    dag_id="IpersonOrclPostTest", 
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "iperson", "test", "DbSync"],
    schedule_interval='50 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *', 
    catchup=False
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_iperson",
        tableNameMaster="iperson", dbMaster="Orcl", dbSlave="Post",
        retryMode="rare",
    )
    addPauseWatcher([task])