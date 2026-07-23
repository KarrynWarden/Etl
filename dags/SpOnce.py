import datetime as dt
import logging
import colorlog
import sys

sys.path.append("../..")  # добавляем две родительские папки в sys.path

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from Functions.spEtlNew import SpEtl
from Functions.functionsFile.takeOneQuery import TakeOneQuery
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


def do_etl_sp_one(spNameDb):
    """Разовый перенос ОДНОЙ таблицы. Каждая таблица — своя задача, поэтому
    Airflow гоняет их ПАРАЛЛЕЛЬНО (в пределах пула/parallelism), а не по очереди."""
    action = "EtlSpOnce"
    arrSp = assemble("SpOnce")["data"]
    entry = arrSp[spNameDb]

    # Метка -> имя ведущей + направление (срез, как в регулярном справочнике).
    tableNameMaster = spNameDb[:-8]
    dbMaster = spNameDb[-8:-4]
    dbSlave = spNameDb[-4:]
    addSql = entry.get('addSql', '')
    selectSql = entry.get('selectSql', '')
    tableNameSlave = entry.get('tableNameSlave', '')

    if selectSql:
        selectSql = TakeOneQuery(resolvePath(selectSql))
    else:
        selectSql = selectAllSpSql.format(tableNameMaster)

    # Режим разового переноса: 'append' — дополнить ведомую без очистки (SQL обычно
    # с WHERE); иначе 'once' — полная перезаливка (очистить + залить).
    mode = 'append' if entry.get('append') else 'once'

    SpEtl(
        tableNameMaster,
        tableNameSlave,
        TakeOneQuery(resolvePath(addSql)),
        action,
        selectSql,
        dbMaster,
        dbSlave,
        mode,
    )


# Список таблиц разового переноса читаем на этапе сборки дага (только json, без БД),
# чтобы создать по задаче на таблицу — тогда они выполняются параллельно.
try:
    _spOnce = assemble("SpOnce")["data"]
except Exception as _e:  # битый фрагмент не должен ронять парсинг всего дага
    logging.getLogger('airflow.task').error(f"SpOnce: не удалось собрать конфиг: {_e}")
    _spOnce = {}

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

    for _spNameDb, _entry in _spOnce.items():
        # Отключённую таблицу в разовый прогон не включаем (симметрично регулярному).
        if _entry.get('disabled'):
            continue
        PythonOperator(
            task_id=f'once_{_spNameDb}',
            python_callable=do_etl_sp_one,
            op_args=[_spNameDb],
            dag=dag,
        )
