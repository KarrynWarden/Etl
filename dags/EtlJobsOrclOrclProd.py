"""DAG: oracle -> oracle (миграция внутри одного экземпляра)."""
import datetime as dt

from airflow.models import DAG

from dags._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl

with DAG(
    dag_id="EtlJobsOrclOrclProd",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclOrcl", "etljobs", "prod", "DbSync"],
    schedule_interval=dt.timedelta(minutes=10),
    catchup=False,
) as dag:
    configureLogger()
    do_etl = buildOperator(
        "do_etl",
        runEtl(tableNameMaster="etljobs", dbMaster="Orcl", dbSlave="Orcl"),
    )
    do_etl
