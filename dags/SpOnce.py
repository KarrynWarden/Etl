import datetime as dt
import logging
import colorlog
import sys
import json

sys.path.append("../..")  # добавляем две родительские папки в sys.path

from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException
from Functions.spEtlOnce import SpEtl
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH
from Src.spQueries import selectAllSpSql
from Functions.functionsFile.loadConfig import assemble, resolvePath

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 10, 23),
    'retries': 5,
    'depends_on_past': True,
    'retry_delay': dt.timedelta(minutes=1),
    'timezone': 'Asia/Yekaterinburg',
}

def do_etl_sp():
    action = "EtlSpOnce"
    arrSp = assemble("SpOnce")["data"]
    for spNameDb in list(arrSp.keys()):
        tableNameMaster = spNameDb[:-8]
        dbMaster = spNameDb[-8:-4]
        dbSlave = spNameDb[-4:]
        addSql = arrSp[spNameDb].get('addSql', '')
        selectSql = arrSp[spNameDb].get('selectSql', '')
        tableNameSlave = arrSp[spNameDb].get('tableNameSlave', '')
        if selectSql:
            selectSql = TakeOneQuery(resolvePath(selectSql))
        else:
            selectSql = selectAllSpSql.format(tableNameMaster)
        mode = 'once'
        SpEtl(tableNameMaster, tableNameSlave, TakeOneQuery(resolvePath(addSql)), action, selectSql, dbMaster, dbSlave, mode)

with DAG(dag_id='SpEtlOnce',
         default_args=args,
         max_active_runs=1,
         tags=["OrclPost", "PostOrcl", "spetl", "once", "DbSync"],
         schedule_interval=None,
         catchup=False) as dag:

    logger = logging.getLogger('airflow.task')
    handler = logging.StreamHandler()
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={
            'DEBUG': 'reset',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    do_etl_sp = PythonOperator(
        task_id='do_etl_sp',
        provide_context=True,
        python_callable=do_etl_sp,
        dag=dag
    )

    do_etl_sp