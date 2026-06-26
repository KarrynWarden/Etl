import datetime as dt
import logging
import colorlog
import sys
import json

sys.path.append("../..")  # добавляем две родительские папки в sys.path

from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException
from Functions.spEtlNew import SpEtl
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH
from Src.spQueries import selectAllSpSql
from Connect import DbConnectOrcl, DbConnectPost

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 10, 23),
    'retries': 5,
    'depends_on_past': True,
    'retry_delay': dt.timedelta(minutes=1),
    'timezone': 'Asia/Yekaterinburg',
}

def do_etl_sp():
    action = "EtlSp"
    with open(f'{FULL_PATH}etlFolder/SpTableName.json') as file:
        data = json.load(file)
    arrSp = data['data']

    # Создаем dependence_map с информацией о dbMaster для каждой зависимости
    dependence_map = {}
    for spNameDb in list(arrSp.keys()):
        tableNameMaster = spNameDb[:-8]
        if tableNameMaster[-1] in ['1','2']:
            tableNameMaster = tableNameMaster[:-1]
        dependence = arrSp[spNameDb].get('dependence', '')
        if not dependence:
            dependence = tableNameMaster
            
        dbMaster = spNameDb[-8:-4]  # Определяем источник зависимости
        
        if dependence not in dependence_map:
            dependence_map[dependence] = {'dbMaster': dbMaster, 'spNames': []}
        # Проверка согласованности источника для одной зависимости
        elif dependence_map[dependence]['dbMaster'] != dbMaster:
            raise ValueError(f"Ошибка конфигурации: зависимость {dependence} имеет разные источники (Post/Oracle)")
            
        dependence_map[dependence]['spNames'].append(spNameDb)

    # Получаем зависимости из Postgres
    post_dependencies = []
    try:
        con_post = DbConnectPost()
        cursor_post = con_post.cursor()
        cursor_post.execute("SELECT DISTINCT tablename FROM etl_user.etl_jobs WHERE isokaudit = 0")
        post_dependencies = [row[0] for row in cursor_post.fetchall()]
        cursor_post.close()
        con_post.close()
    except Exception as e:
        print(f"Ошибка при получении зависимостей из Postgres: {str(e)}")

    # Получаем зависимости из Oracle
    orcl_dependencies = []
    try:
        con_orcl = DbConnectOrcl()
        cursor_orcl = con_orcl.cursor()
        cursor_orcl.execute("SELECT DISTINCT tablename FROM koknaev.etl_jobs WHERE isokaudit = 0")
        orcl_dependencies = [row[0] for row in cursor_orcl.fetchall()]
        cursor_orcl.close()
        con_orcl.close()
    except Exception as e:
        print(f"Ошибка при получении зависимостей из Oracle: {str(e)}")

    # Объединяем зависимости с указанием источника
    dependencies_to_process = []
    for dep in post_dependencies:
        if dep in dependence_map and dependence_map[dep]['dbMaster'] == "Post":
            dependencies_to_process.append((dep, "Post"))
    for dep in orcl_dependencies:
        if dep in dependence_map and dependence_map[dep]['dbMaster'] == "Orcl":
            dependencies_to_process.append((dep, "Oracle"))

    # Обрабатываем каждую зависимость
    counter = 0
    for dependence, db_type in dependencies_to_process:
        if dependence in dependence_map:
            print(f'Начало обработки зависимости {dependence} (источник: {db_type})')
            counter += 1
            spNames = dependence_map[dependence]['spNames']
            all_success = True
            results = []

            # Обрабатываем все таблицы, зависящие от этой зависимости
            for spNameDb in spNames:
                tableNameMaster = spNameDb[:-8]
                if tableNameMaster[-1] in ['1','2']:
                    tableNameMaster = tableNameMaster[:-1]
                dbMaster = spNameDb[-8:-4]
                dbSlave = spNameDb[-4:]
                addSql = arrSp[spNameDb].get('addSql', '')
                selectSql = arrSp[spNameDb].get('selectSql', '')
                tableNameSlave = arrSp[spNameDb].get('tableNameSlave', '')

                if selectSql:
                    selectSql = TakeOneQuery(selectSql)
                else:
                    selectSql = selectAllSpSql.format(tableNameMaster)

                # Выполняем ETL
                success = SpEtl(
                    tableNameMaster,
                    tableNameSlave,
                    TakeOneQuery(addSql),
                    action,
                    selectSql,
                    dbMaster,
                    dbSlave,
                    'regular'
                )
                results.append((spNameDb, success))
                if not success:
                    all_success = False

            # Обновляем etl_jobs в соответствующей БД
            if all_success and results:
                try:
                    if db_type == "Post":
                        con_etl = DbConnectPost()
                        cursor_etl = con_etl.cursor()
                        cursor_etl.execute(
                            "UPDATE etl_user.etl_jobs SET isokaudit = 1, last_success_ts = CURRENT_TIMESTAMP WHERE tablename = %s",
                            (dependence,)
                        )
                    else:  # Oracle
                        con_etl = DbConnectOrcl()
                        cursor_etl = con_etl.cursor()
                        cursor_etl.execute(
                            "UPDATE koknaev.etl_jobs SET isokaudit = 1, last_success_ts = CURRENT_TIMESTAMP WHERE tablename = :1",
                            (dependence,)
                        )
                    con_etl.commit()
                    print(f"Статус isokaudit обновлен для {dependence} в {db_type}")
                except Exception as e:
                    print(f"Ошибка обновления etl_jobs для {dependence}: {str(e)}")
                    all_success = False
                finally:
                    if 'cursor_etl' in locals():
                        cursor_etl.close()
                    if 'con_etl' in locals():
                        con_etl.close()
            else:
                failed_tables = [sp for sp, succ in results if not succ]
                print(f"Ошибка обновления зависимости {dependence} для таблиц: {failed_tables}")

    if counter == 0:
        raise AirflowSkipException

with DAG(dag_id='SpEtlNew',
         default_args=args,
         max_active_runs=1,
         tags=["OrclPost", "PostOrcl", "spetl", "DbSync"],
         schedule_interval=dt.timedelta(minutes=5),
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