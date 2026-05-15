"""Общая обвязка для DAG'ов: однотипный логгер и фабрика задачи."""
import datetime as dt
import logging

import colorlog
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.operators.python_operator import PythonOperator

from Functions.do_etl import Do_etl


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 10, 23),
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=1),
    "depends_on_past": True,
    "timezone": "Asia/Yekaterinburg",
}


def configureLogger():
    logger = logging.getLogger("airflow.task")
    handler = logging.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        log_colors={
            "DEBUG": "reset",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    ))
    logger.addHandler(handler)
    return logger


def runEtl(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None, **opts):
    """Универсальный запуск ETL: тонкая обёртка для PythonOperator."""
    def _task():
        try:
            logging.info("Старт ETL %s %s->%s", tableNameMaster, dbMaster, dbSlave)
            Do_etl(
                tableNameMaster=tableNameMaster,
                dbMaster=dbMaster,
                dbSlave=dbSlave,
                tableNameEtlJobs=tableNameEtlJobs,
                **opts,
            )
        except AirflowSkipException:
            raise
        except Exception as err:
            raise AirflowException(f"Ошибка при выполнении дага: {err}")
    return _task


def buildOperator(taskId, callable_, triggerRule=None):
    """Создать PythonOperator для одной задачи ETL.

    triggerRule — опциональный airflow trigger_rule (например, "all_done").
    По умолчанию airflow использует "all_success" — это значит, что если
    upstream вылетел с AirflowSkipException, текущая задача тоже скипнется
    каскадно. В DAG'ах с несколькими независимыми задачами (mocheck по 9
    doctype'ам, medree по двум таблицам) удобнее "all_done" — каждая
    задача запускается независимо и независимо красится в нужный цвет.
    """
    kwargs = dict(
        task_id=taskId,
        pool="Prod",
        provide_context=True,
        priority_weight=1,
        python_callable=callable_,
    )
    if triggerRule is not None:
        kwargs["trigger_rule"] = triggerRule
    return PythonOperator(**kwargs)
