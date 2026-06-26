import sys
import datetime as dt
import logging
import colorlog

sys.path.append("../..")  # добавляем две родительские папки в sys.path

from datetime import datetime
from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from Functions.do_etl_new import Do_etl
from airflow.exceptions import AirflowSkipException, AirflowException
from Src.fullPath import FULL_PATH
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Functions.functionsFile.loadConfig import LoadConfig
from Src.generalQueries import structureEmptyQuerySql

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 10, 23),
    'retries': 5,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': True,
    'timezone': 'Asia/Yekaterinburg',
}

def do_etl():
    try:
        logging.info('Выполнение Dag - PrbsmoPostOrcl') # заголовок процесса !ИЗМЕНИТЬ ДЛЯ НОВЫХ ДАГОВ!
        tableNameMaster = 'prbsmo'
        dbMaster = 'Post'
        dbSlave = 'Orcl'
        
        #print(tableNameMaster+dbMaster+dbSlave)
        tableConfig = LoadConfig(tableNameMaster+dbMaster+dbSlave)
        tableNameSlave = tableConfig['tableNameSlave'] #название ведомой таблицы
        structureMaster = tableConfig['structureMaster'] #структура ведущей таблицы 
        structureSlave = tableConfig['structureSlave'] #структура ведомой таблицы 
        if 'selectSql' in tableConfig: #если есть запрос пользователя, то берём его. Иначе считается, что источником данных является таблица 
            selectSql = TakeOneQuery(tableConfig['selectSql'])
        else:
            selectSql =  structureEmptyQuerySql.format(tableNameMaster)
        #запуск функции do_etl
        Do_etl(tableNameMaster, tableNameSlave, structureMaster, structureSlave, selectSql, dbMaster, dbSlave)
    except AirflowSkipException:
        raise AirflowSkipException
    except Exception as error:
        raise AirflowException(f"Ошибка при выполнении дага: {str(error)}")

with DAG(dag_id='PrbsmoPostOrcl', # id дага !ИЗМЕНИТЬ ДЛЯ НОВЫХ ДАГОВ!
         default_args=args,
         max_active_runs=1,
         tags=["PostOrcl", "prbsmo", "", "DbSync", "A83"], # тэги дага !ИЗМЕНИТЬ ДЛЯ НОВЫХ ДАГОВ!
         schedule_interval=dt.timedelta(minutes=1),
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
    do_etl = PythonOperator(
        task_id='do_etl',
        pool="Etl",
        provide_context=True,
        priority_weight=1,
        python_callable=do_etl,
        dag=dag
    )
    do_etl