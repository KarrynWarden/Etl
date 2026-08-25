import os
import time
import shutil
from datetime import datetime, timedelta
import datetime as dt
import logging
import colorlog

from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException
from Connect import DbConnectPost, DbConnectOrcl

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 10, 23),
    'retries': 5,
    'depends_on_past': True,
    'retry_delay': dt.timedelta(minutes=1),
    # пояс не задаём намеренно — см. Functions/_dagHelpers.py,
    # кроны здесь написаны по UTC (5:50 = 10:50 в Екатеринбурге)
}

def audit_review():
    directory = os.path.join(os.path.dirname(__file__), '../logs')
    now = time.time()
    for dag_id in os.listdir(directory):
        dag_path = os.path.join(directory, dag_id)
        if os.path.isdir(dag_path):
            # Set retention period: 1 day for test1, 30 days for others
            # Эти 2 работют каждые 10 секунд и делают кучу логов
            retention_days = 0 if dag_id == 'dag_id=A56QeventFromQpersonInsertPROD' or dag_id == 'dag_id=A56QeventFromQpersonInsert' else 15 
            threshold_time = now - timedelta(days=retention_days).total_seconds()

            for subfolder in os.listdir(dag_path):
                subfolder_path = os.path.join(dag_path, subfolder)
                if os.path.isdir(subfolder_path):
                    folder_mod_time = os.path.getmtime(subfolder_path)
                    if folder_mod_time < threshold_time:
                        shutil.rmtree(subfolder_path)

    # Database cleanup remains at 30 days for all entries
    con = DbConnectPost()
    cursor = con.cursor()
    date_30_days_ago = datetime.now() - timedelta(days=15)
    cursor.execute("DELETE FROM etl_log_iud_row WHERE timeoper < %s", (date_30_days_ago,))
    con.commit()
    cursor.close()
    con.close()

with DAG(dag_id='DeleteDag',
         default_args=args,
         max_active_runs=1,
         tags=["delete", "DbSync"],
         schedule_interval='0 21 * * *',
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

    audit_review = PythonOperator(
        task_id='audit_review',
        provide_context=True,
        pool="Prod",
        priority_weight=2,
        pool_slots=100,
        python_callable=audit_review,
        dag=dag
    )

    audit_review
