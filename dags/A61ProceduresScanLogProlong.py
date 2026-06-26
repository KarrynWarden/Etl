import datetime as dt
import logging
import colorlog
import sys
import json

sys.path.append("../..")  # добавляем две родительские папки в sys.path

from Connect import DbConnectPost
from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 10, 23),
    'retries': 5,
    'depends_on_past': True,
    'retry_delay': dt.timedelta(minutes=1),
    'timezone': 'Asia/Yekaterinburg',
}

def do_etl_procedures():
    action = "Procedure"
    try:
        con = DbConnectPost()
        cursor = con.cursor()
        cursor.execute("call pck_eindexmo.ScanLogProlong()")
        con.commit()
        
    except Exception as error:
        print('При выполнении произошла критическая ошибка..........', error)
    finally:
        try:
            con
        except NameError:
            print(f"соединение Postgres не было открыто")
        else:
            cursor.close()
            con.close()
            print(f"соединение Postgres закрыто")

with DAG(dag_id='A61ProceduresScanLogProlong',
         default_args=args,
         max_active_runs=1,
         tags=["A61", "procedures", "DbSync"],
         schedule_interval=dt.timedelta(minutes=60),
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

    do_etl_procedures = PythonOperator(
        task_id='do_etl_procedures',
        provide_context=True,
        python_callable=do_etl_procedures,
        dag=dag
    )

    do_etl_procedures