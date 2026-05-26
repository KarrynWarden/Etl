"""Общая обвязка для DAG'ов: однотипный логгер и фабрика задачи."""
import datetime as dt
import logging
import time

import colorlog
from airflow.exceptions import AirflowSkipException, AirflowException, AirflowFailException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from airflow.utils.timezone import utcnow
from airflow.utils.session import create_session
from airflow.models import TaskInstance, DagRun, DagModel
from urllib.parse import quote
from airflow.configuration import conf
from Src.fullPath import WEB_BASE_URL

from Functions.do_etl import Do_etl


#DEFAULT_ARGS = {
#    "owner": "airflow",
#    "start_date": dt.datetime(2023, 10, 23),
#    "retries": 1,
#    "retry_delay": dt.timedelta(minutes=1),
#    "depends_on_past": False,
#    "timezone": "Asia/Yekaterinburg",
#}

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 10, 23),
    "timezone": "Asia/Yekaterinburg",
    # retries / retry_delay / depends_on_past убраны намеренно:
    # ретраи задаются per-mode (см. makeEtlOperator), заморозка упавшей
    # линии — в runEtl. depends_on_past вешал бы весь DAG-run.
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


def buildOperator(taskId, callable_, triggerRule=None):
    kwargs = dict(
        task_id = taskId, 
        pool="Test",
        provide_context=True,
        priority_weight=1,
        python_callable=callable_,
    )
    if triggerRule is not None:
        kwargs["trigger_rule"] = triggerRule
    return PythonOperator(**kwargs)
    #return PythonOperator(
    #    task_id=taskId,
    #    pool="Test",
    #    provide_context=True,
    #    priority_weight=1,
    #    python_callable=callable_,
    #)

#def _previousRunFailed(context):
#    """True, если эта же задача в прошлом DAG-run завершилась ошибкой."""
#    prev = context["ti"].get_previous_ti()
#    return prev is not None and prev.state == State.FAILED

#def runEtl(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None, **opts):
#    line = tableNameEtlJobs or tableNameMaster

#    def _task(**context):
        # Защита «упавшая линия»: если прошлый запуск этой задачи упал —
        # сразу падаем без ретраев и без обращения к БД. Линия стоит, пока
        # человек не пометит упавшую задачу как success в airflow UI.
#        if _previousRunFailed(context):
#            raise AirflowFailException(
#                f"Линия {line} заморожена: прошлый запуск завершился "
#                f"ошибкой. Разморозка — mark success на упавшей задаче."
#            )
#        try:
#            Do_etl(tableNameMaster=tableNameMaster, dbMaster=dbMaster,
#                   dbSlave=dbSlave, tableNameEtlJobs=tableNameEtlJobs, **opts)
#        except (AirflowSkipException, AirflowFailException):
#            raise
#        except Exception as err:
#            raise AirflowException(f"Ошибка при выполнении дага: {err}")
#    return _task


# Паузы (мин) после падений №1..4; после 5-го падения — FROZEN.
FREQUENT_BACKOFF_MIN = [1, 1]

def _taskHistory(context, limit=150):
    """История этой задачи (state, end_date, run_id), новые — первыми."""
    ti = context["ti"]
    with create_session() as session:
        return (
            session.query(TaskInstance.state, TaskInstance.end_date,
                           DagRun.run_id, DagRun.execution_date)
            .join(DagRun, (DagRun.dag_id == TaskInstance.dag_id)
                          & (DagRun.run_id == TaskInstance.run_id))
            .filter(TaskInstance.dag_id == ti.dag_id,
                    TaskInstance.task_id == ti.task_id,
                    DagRun.execution_date < context["dag_run"].execution_date)
            .order_by(DagRun.execution_date.desc())
            .limit(limit)
            .all()
        )

def _retryDecision(context):
    """Решение FREQUENT-режима. Возвращает (action, info).

    action: 'run' — выполнять Do_etl; 'wait' — ещё рано (skip);
            'frozen' — серия исчерпана (fail без работы).
    """
    failed = []
    for state, endDate, runId, execDate in _taskHistory(context):
        if state == State.FAILED:
            failed.append((endDate, runId, execDate))
        elif state == State.SKIPPED:
            continue
        else:
            break

    if not failed:
        return "run", {"attempt": 1}

    failCount = len(failed)
    firstFailRunId = failed[-1][1]    # самое первое падение серии — для ссылки
    firstFailExecDate = failed[-1][2]

    if failCount > len(FREQUENT_BACKOFF_MIN):
        return "frozen", {"failCount": failCount, "firstFailRunId": firstFailRunId, "firstFailExecDate":firstFailExecDate}

    waitMin = FREQUENT_BACKOFF_MIN[failCount - 1]
    lastFailEnd = failed[0][0]
    dueAt = lastFailEnd + dt.timedelta(minutes=waitMin)
    info = {"attempt": failCount + 1, "failCount": failCount,
            "waitMin": waitMin, "lastFailEnd": lastFailEnd,
            "dueAt": dueAt, "firstFailRunId": firstFailRunId, "firstFailExecDate":firstFailExecDate}
    return ("run" if utcnow() >= dueAt else "wait"), info


def _logsUrl(context, runId, execDate=None):
    """Ссылка на логи задачи в указанном запуске.

    base_date нужен, иначе grid airflow показывает только последние 25
    запусков от текущего времени — и старый run на странице не появится,
    ссылка откроет просто DAG, а не лог.
    """
    base = (WEB_BASE_URL or conf.get("webserver", "base_url")).rstrip("/")
    ti = context["ti"]
    parts = [f"dag_run_id={quote(runId, safe='')}",
             f"task_id={ti.task_id}",
             "tab=logs"]
    if execDate is not None:
        # +1 мин — чтобы целевой run гарантированно попал в окно «25 последних до base_date»
        anchor = (execDate.astimezone(dt.timezone.utc)
                  + dt.timedelta(minutes=1)
                 ).strftime("%Y-%m-%dT%H:%M:%SZ")
        parts.insert(0, f"base_date={quote(anchor, safe='')}")
    return f"{base}/dags/{ti.dag_id}/grid?" + "&".join(parts)


def _previousRunFailed(context):
    firstFailRunId = None
    firstFailExecDate = None
    for state, _end, runId, execDate in _taskHistory(context):
        if state == State.FAILED:
            firstFailRunId = runId
            firstFailExecDate = execDate
        elif state == State.SUCCESS:
            break
    return (firstFailRunId is not None, firstFailRunId, firstFailExecDate)
    

def runEtl(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None,
           retryMode="frequent", **opts):
    """Обёртка ETL-задачи: ретраи + заморозка упавшей линии.

    retryMode='frequent' — кастомный FSM по тикам расписания (_retryDecision);
    retryMode='rare'     — ретраи делает airflow; если прошлый запуск всё
                           равно упал — линия заморожена.
    """
    line = tableNameEtlJobs or tableNameMaster

    def _task(**context):
        if retryMode == "frequent":
            action, info = _retryDecision(context)
            if action in ("wait", "frozen"):
                url = _logsUrl(context, info["firstFailRunId"], info["firstFailExecDate"])
                msg = (f"линия {line}, падений {info['failCount']}, "
                       f"backoff {FREQUENT_BACKOFF_MIN} мин; "
                       )
                firstErr = f"Первая ошибка: {url}"
                if action == "wait":
                    logging.warning("ОЖИДАНИЕ РЕТРАЯ — %s; следующий не "
                                    "раньше %s", msg, info["dueAt"])
                    logging.warning(firstErr)
                    raise AirflowSkipException
                logging.error("ЛИНИЯ ЗАМОРОЖЕНА — %s; разморозка: mark "
                              "success на упавшей задаче", msg)
                logging.error(firstErr)
                raise AirflowFailException(f"Линия {line} заморожена")
            logging.info("ЗАПУСК ETL — линия %s, попытка %s",
                         line, info["attempt"])
        else:  # rare
            failed, firstRunId, firstExecDate = _previousRunFailed(context)
            if failed:
                logging.error("ЛИНИЯ ЗАМОРОЖЕНА — линия %s;"
                              " разморозка: mark success", line,
                              url = _logsUrl(context, firstRunId, firstExecDate))
                logging.error("Первая ошибка: "
                              "%s; ", line,
                              url = _logsUrl(context, firstRunId, firstExecDate))
                raise AirflowFailException(f"Линия {line} заморожена")

        try:
            Do_etl(tableNameMaster=tableNameMaster, dbMaster=dbMaster,
                   dbSlave=dbSlave, tableNameEtlJobs=tableNameEtlJobs, **opts)
        except (AirflowSkipException, AirflowFailException):
            raise
        except Exception as err:
            raise AirflowException(f"Ошибка ETL линии {line}: {err}")
    return _task

# airflow-ретраи для RARE-режима: задержки 1,2,4,8,16,30 мин (потолок 30).
_RARE_RETRY_ARGS = dict(
    retries=6,
    retry_delay=dt.timedelta(minutes=1),
    retry_exponential_backoff=True,
    max_retry_delay=dt.timedelta(minutes=30),
)

def makeEtlOperator(taskId, tableNameMaster, dbMaster, dbSlave,
                    tableNameEtlJobs=None, retryMode="frequent",
                    triggerRule=None, **opts):
    """Собрать PythonOperator ETL-линии целиком (callable + ретраи).

    retryMode='frequent' — airflow не ретраит (retries=0); ретраи делает
                           FSM в runEtl по тикам расписания;
    retryMode='rare'     — ретраи делает airflow (экспонента 1..30 мин).
    """
    kwargs = dict(
        task_id=taskId,
        pool="Prod",
        provide_context=True,
        priority_weight=1,
        python_callable=runEtl(tableNameMaster, dbMaster, dbSlave,
                               tableNameEtlJobs, retryMode=retryMode, **opts),
    )
    if retryMode == "rare":
        kwargs.update(_RARE_RETRY_ARGS)
    else:
        kwargs["retries"] = 0
    if triggerRule is not None:
        kwargs["trigger_rule"] = triggerRule
    return PythonOperator(**kwargs)


def _pauseWatcher(**context):
    """Если ВСЕ линии дага в этом запуске упали — ставит DAG на паузу.

    Защита от бесконечного накопления полностью-красных запусков, когда
    сломаны все линии разом. Текущий (последний) запуск с ошибками и
    ссылками остаётся наверху. Возобновление — снять DAG с паузы вручную
    + пометить линии успешными.
    """
    dagRun = context["dag_run"]
    ti = context["ti"]
    lineTis = [t for t in dagRun.get_task_instances()
               if t.task_id != ti.task_id]
    if lineTis and all(t.state == State.FAILED for t in lineTis):
        with create_session() as session:
            session.query(DagModel).filter(
                DagModel.dag_id == ti.dag_id
            ).update({"is_paused": True})
        logging.error("Все линии дага %s заморожены — DAG поставлен на "
                      "паузу. Возобновление: снять с паузы + mark success "
                      "на линиях.", ti.dag_id)
    else:
        logging.info("Watcher: живые линии есть — даг продолжает работу.")

def addPauseWatcher(lineTasks):
    """Добавить задачу-сторож после всех линий дага."""
    watcher = PythonOperator(
        task_id="freeze_watcher",
        pool="Prod",
        provide_context=True,
        python_callable=_pauseWatcher,
        trigger_rule="all_done",
    )
    for task in lineTasks:
        task >> watcher
    return watcher



WATCHER_RECHECK_SEC = 60

def _taskHistory(context, taskId=None, limit=150):
    """История задачи (state, end_date, run_id, exec_date), новые первыми.
    taskId — по умолчанию текущая задача; watcher передаёт чужие линии."""
    ti = context["ti"]
    taskId = taskId or ti.task_id
    with create_session() as session:
        return (
            session.query(TaskInstance.state, TaskInstance.end_date,
                           DagRun.run_id, DagRun.execution_date)
            .join(DagRun, (DagRun.dag_id == TaskInstance.dag_id)
                          & (DagRun.run_id == TaskInstance.run_id))
            .filter(TaskInstance.dag_id == ti.dag_id,
                    TaskInstance.task_id == taskId,
                    DagRun.execution_date < context["dag_run"].execution_date)
            .order_by(DagRun.execution_date.desc())
            .limit(limit)
            .all()
        )

def _streakLen(context, taskId):
    """Длина серии ведущих падений линии (skipped прозрачны)."""
    n = 0
    for state, _end, _runId, _ed in _taskHistory(context, taskId=taskId):
        if state == State.FAILED:
            n += 1
        elif state == State.SKIPPED:
            continue
        else:
            break
    return n

def _allLinesFrozen(context, retryMode):
    """True, только если ВСЕ линии исчерпали ретраи (с учётом текущего run)."""
    lines = _lineStates(context)
    if not lines:
        return False
    for taskId, state in lines:
        if state != State.FAILED:
            return False
        if retryMode == "frequent":
            if _streakLen(context, taskId) + 1 <= len(FREQUENT_BACKOFF_MIN):
                return False
    return True

def _freezeWatcher(retryMode):
    def _watch(**context):
        # 1) все линии заморожены → удерживаем DAG-run (бесконечный даг)
        if _allLinesFrozen(context, retryMode):
            logging.error("Watcher: ВСЕ линии заморожены. Удерживаю DAG-run "
                          "(новые запуски не плодятся). Разморозка — mark "
                          "success на упавших линиях этого запуска.")
            while _allLinesFrozen(context, retryMode):
                time.sleep(WATCHER_RECHECK_SEC)
            logging.info("Watcher: линии разморожены — отпускаю DAG-run.")
            return
        # 2) не все заморожены, но есть упавшие → сводный статус run'а в 🟥
        failed = [tid for tid, st in _lineStates(context)
                  if st == State.FAILED]
        if failed:
            raise AirflowException(
                f"В запуске есть упавшие линии: {', '.join(failed)}. "
                f"Сводный статус — ошибка; ретраи и причина — в самих линиях."
            )
        logging.info("Watcher: упавших линий нет — даг зелёный.")
    return _watch

def addFreezeWatcher(lineTasks, retryMode="frequent"):
    """Сторож после всех линий. Если все линии исчерпали ретраи —
    «зависает», удерживая DAG-run открытым: max_active_runs=1 не даёт
    плодить новые красные запуски, а DAG остаётся в фильтре активных."""
    watcher = PythonOperator(
        task_id="freeze_watcher",
        provide_context=True,
        python_callable=_freezeWatcher(retryMode),
        trigger_rule="all_done",
    )
    for task in lineTasks:
        task >> watcher
    return watcher

def _lineStates(context):
    """[(task_id, state)] линий текущего запуска (watcher исключён)."""
    ti = context["ti"]
    dagRun = context["dag_run"]
    with create_session() as session:
        return [(t.task_id, t.state)
                for t in dagRun.get_task_instances(session=session)
                if t.task_id != ti.task_id]


