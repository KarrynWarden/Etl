"""Общая обвязка для DAG'ов: логгер, фабрики задач (runEtl, makeEtlOperator),
FSM-ретраев по тикам расписания, watcher с авто-удержанием при полной
заморозке, ссылки на первую ошибку.

Классы ошибок (записываются в XCom 'error_class' на упавшей задаче):
  - 'record'    — ошибка по конкретной записи (Integrity/Data/неизвестное):
                   запись припаркована (isetl=-1), линия НЕ морозится, но
                   квадрат 🟥 для видимости в общем списке;
  - 'retryable' — соединение умерло (Operational/Interface) — FSM считает
                   в backoff-серию;
  - 'fatal'     — структурная (Programming, FLK) — FSM сразу frozen.

SIGTERM при перезапуске airflow специально конвертируется в SkipException
(🟪) — не загрязняет историю ложными красными квадратами при деплое.
"""
import datetime as dt
import logging
import time
from urllib.parse import quote

import colorlog
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowSkipException, AirflowException, AirflowFailException,
)
from airflow.models import TaskInstance, DagRun, DagModel, XCom
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import utcnow

from Functions.do_etl import Do_etl, RecordScopeError, classifyError
from Src.fullPath import WEB_BASE_URL


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 10, 23),
    "timezone": "Asia/Yekaterinburg",
    # retries / retry_delay / depends_on_past убраны намеренно:
    # ретраи — per-mode (см. makeEtlOperator), заморозка — в runEtl.
    # depends_on_past вешал бы весь DAG-run в нетерминальном состоянии.
}


# Паузы (мин) после падений №1..N; после (N+1)-го — FROZEN.
FREQUENT_BACKOFF_MIN = [1, 1]

# Период перепроверки watcher'ом при удержании "бесконечного дага".
WATCHER_RECHECK_SEC = 60

# airflow-ретраи для RARE-режима: задержки ≈ 1, 2, 4, 8, 16, 30 мин (потолок 30).
_RARE_RETRY_ARGS = dict(
    retries=6,
    retry_delay=dt.timedelta(minutes=1),
    retry_exponential_backoff=True,
    max_retry_delay=dt.timedelta(minutes=30),
)


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


# ----------------------------------------------------------------------------
#               История задачи + класс ошибки из XCom
# ----------------------------------------------------------------------------

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


def _xcomErrorClass(dagId, taskId, runId):
    """Прочитать XCom 'error_class' конкретного TI. None если нет."""
    try:
        with create_session() as session:
            return XCom.get_one(
                run_id=runId,
                task_id=taskId,
                dag_id=dagId,
                key="error_class",
                session=session,
            )
    except Exception:
        return None


# ----------------------------------------------------------------------------
#                       FSM ретраев (FREQUENT-режим)
# ----------------------------------------------------------------------------

def _retryDecision(context):
    """Решение FREQUENT-режима. Возвращает (action, info).

    action: 'run'   — выполнять Do_etl;
            'wait'  — ещё рано (skip);
            'frozen'— серия исчерпана или fatal-ошибка (fail без работы).

    Логика серии:
    - RECORD-падения прозрачны (не считаются, серию не рвут) — линия
      продолжает работать как ни в чём не бывало.
    - FATAL-падение сразу даёт frozen, ретраи бессмысленны.
    - RETRYABLE / неклассифицированное — копится в серию по backoff.
    """
    ti = context["ti"]
    failed = []
    for state, endDate, runId, execDate in _taskHistory(context):
        if state == State.FAILED:
            cls = _xcomErrorClass(ti.dag_id, ti.task_id, runId)
            if cls == "record":
                continue                  # прозрачно — это запись, не линия
            if cls == "fatal":
                # фатальная ошибка — сразу заморозка, ретраи не нужны
                return "frozen", {
                    "failCount": 1,
                    "firstFailRunId": runId,
                    "firstFailExecDate": execDate,
                    "errorClass": "fatal",
                }
            failed.append((endDate, runId, execDate))
        elif state == State.SKIPPED:
            continue
        else:
            break

    if not failed:
        return "run", {"attempt": 1}

    failCount = len(failed)
    firstFailRunId = failed[-1][1]
    firstFailExecDate = failed[-1][2]

    if failCount > len(FREQUENT_BACKOFF_MIN):
        return "frozen", {
            "failCount": failCount,
            "firstFailRunId": firstFailRunId,
            "firstFailExecDate": firstFailExecDate,
            "errorClass": "retryable",
        }

    waitMin = FREQUENT_BACKOFF_MIN[failCount - 1]
    lastFailEnd = failed[0][0]
    dueAt = lastFailEnd + dt.timedelta(minutes=waitMin)
    info = {
        "attempt": failCount + 1, "failCount": failCount,
        "waitMin": waitMin, "lastFailEnd": lastFailEnd, "dueAt": dueAt,
        "firstFailRunId": firstFailRunId,
        "firstFailExecDate": firstFailExecDate,
    }
    return ("run" if utcnow() >= dueAt else "wait"), info


# ----------------------------------------------------------------------------
#                       Ссылка на логи первой ошибки
# ----------------------------------------------------------------------------

def _logsUrl(context, runId, execDate=None):
    """Ссылка на логи задачи в указанном запуске.

    base_date обязателен: иначе grid airflow показывает только последние 25
    запусков от current_time — и старый run будет «вне окна», ссылка
    откроет просто DAG, а не нужный лог.
    """
    base = (WEB_BASE_URL or conf.get("webserver", "base_url")).rstrip("/")
    ti = context["ti"]
    parts = [f"dag_run_id={quote(runId, safe='')}",
             f"task_id={ti.task_id}",
             "tab=logs"]
    if execDate is not None:
        # +1 мин — целевой run гарантированно попадает в окно "25 до base_date"
        anchor = (execDate.astimezone(dt.timezone.utc)
                  + dt.timedelta(minutes=1)
                 ).strftime("%Y-%m-%dT%H:%M:%SZ")
        parts.insert(0, f"base_date={quote(anchor, safe='')}")
    return f"{base}/dags/{ti.dag_id}/grid?" + "&".join(parts)


# ----------------------------------------------------------------------------
#                    RARE-режим: «прошлый запуск упал?»
# ----------------------------------------------------------------------------

def _previousRunFailed(context):
    """Вернуть (failed, firstRunId, firstExecDate) для RARE-режима.

    RECORD-падения прозрачны: не считаются за «упавшую линию».
    FATAL и retryable, после исчерпания airflow-ретраев — считаются.
    """
    ti = context["ti"]
    firstFailRunId = None
    firstFailExecDate = None
    for state, _end, runId, execDate in _taskHistory(context):
        if state == State.FAILED:
            cls = _xcomErrorClass(ti.dag_id, ti.task_id, runId)
            if cls == "record":
                continue
            firstFailRunId = runId
            firstFailExecDate = execDate
        elif state == State.SUCCESS:
            break
    return (firstFailRunId is not None, firstFailRunId, firstFailExecDate)


# ----------------------------------------------------------------------------
#                       Обёртка задачи: runEtl
# ----------------------------------------------------------------------------

def _isSigterm(err):
    return "SIGTERM" in str(err)


def runEtl(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None,
           retryMode="frequent", **opts):
    """Обёртка ETL-задачи: ретраи + заморозка упавшей линии + XCom error_class.

    retryMode='frequent' — кастомный FSM (_retryDecision) по тикам расписания;
    retryMode='rare'     — ретраи делает airflow (экспонента), затем заморозка
                           через _previousRunFailed.

    SIGTERM (перезапуск airflow) специально конвертируется в SkipException —
    не загрязняет историю ложными красными квадратами.
    """
    line = tableNameEtlJobs or tableNameMaster

    def _task(**context):
        # ---------- FREQUENT: FSM-ретраев ----------
        if retryMode == "frequent":
            action, info = _retryDecision(context)
            if action in ("wait", "frozen"):
                url = _logsUrl(context, info["firstFailRunId"],
                               info["firstFailExecDate"])
                msg = (f"линия {line}, падений {info['failCount']}, "
                       f"backoff {FREQUENT_BACKOFF_MIN} мин; "
                       f"первая ошибка: {url}")
                if action == "wait":
                    logging.warning("ОЖИДАНИЕ РЕТРАЯ — %s; следующий не "
                                    "раньше %s", msg, info["dueAt"])
                    raise AirflowSkipException
                why = "FATAL — структурная/SQL-ошибка" \
                    if info.get("errorClass") == "fatal" else "исчерпаны backoff-ретраи"
                logging.error("ЛИНИЯ ЗАМОРОЖЕНА (%s) — %s; разморозка: "
                              "mark success на упавшей задаче", why, msg)
                raise AirflowFailException(f"Линия {line} заморожена")
            logging.info("ЗАПУСК ETL — линия %s, попытка %s",
                         line, info["attempt"])
        else:
            # ---------- RARE: airflow-ретраи + 1-shot freeze ----------
            failed, firstRunId, firstExecDate = _previousRunFailed(context)
            if failed:
                url = _logsUrl(context, firstRunId, firstExecDate)
                logging.error("ЛИНИЯ ЗАМОРОЖЕНА — линия %s; первая ошибка: "
                              "%s; разморозка: mark success на упавшей задаче",
                              line, url)
                raise AirflowFailException(f"Линия {line} заморожена")

        # ---------- Сам ETL + классификация результата ----------
        ti = context["ti"]
        try:
            Do_etl(tableNameMaster=tableNameMaster, dbMaster=dbMaster,
                   dbSlave=dbSlave, tableNameEtlJobs=tableNameEtlJobs, **opts)
        except AirflowSkipException:
            raise
        except RecordScopeError as err:
            # часть записей не перенесена — линию НЕ морозим, квадрат 🟥
            ti.xcom_push(key="error_class", value="record")
            logging.error("RECORD-ошибки в запуске линии %s: %s", line, err)
            raise AirflowException(f"Линия {line}: {err}")
        except AirflowFailException as err:
            # FLK или иное мгновенно-фатальное — будет frozen на следующем тике
            ti.xcom_push(key="error_class", value="fatal")
            logging.error("FATAL в линии %s: %s", line, err)
            raise
        except Exception as err:
            # SIGTERM при перезапуске airflow — это не ошибка ETL, помечаем skip,
            # чтобы деплой не плодил десятки ложно-красных квадратов
            if _isSigterm(err):
                logging.warning("Линия %s: получен SIGTERM (перезапуск "
                                "airflow) — помечаю как skip", line)
                raise AirflowSkipException("Прервано SIGTERM")
            cls = classifyError(err)
            ti.xcom_push(key="error_class", value=cls)
            logging.error("Линия %s (класс=%s): %s", line, cls, err)
            if cls == "fatal":
                raise AirflowFailException(f"Линия {line}: {err}")
            raise AirflowException(f"Линия {line}: {err}")

    return _task


# ----------------------------------------------------------------------------
#                       Сборка PythonOperator
# ----------------------------------------------------------------------------

def buildOperator(taskId, callable_, triggerRule=None):
    """Простой PythonOperator без ETL-обвязки (для редких ручных задач)."""
    kwargs = dict(
        task_id=taskId,
        pool="Test",
        provide_context=True,
        priority_weight=1,
        python_callable=callable_,
    )
    if triggerRule is not None:
        kwargs["trigger_rule"] = triggerRule
    return PythonOperator(**kwargs)


def makeEtlOperator(taskId, tableNameMaster, dbMaster, dbSlave,
                    tableNameEtlJobs=None, retryMode="frequent",
                    triggerRule=None, pool="Prod", **opts):
    """Собрать PythonOperator ETL-линии целиком (callable + параметры ретраев).

    retryMode='frequent' — airflow не ретраит (retries=0), ретраи делает
                           FSM в runEtl по тикам расписания;
    retryMode='rare'     — ретраи делает airflow (экспонента 1..30 мин).
    """
    kwargs = dict(
        task_id=taskId,
        pool=pool,
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


# ----------------------------------------------------------------------------
#                Watcher: авто-удержание дага при полной заморозке
# ----------------------------------------------------------------------------

def _lineStates(context):
    """[(task_id, state)] линий текущего запуска (watcher исключён).
    Перечитывает заново каждый вызов — человек мог пометить success."""
    ti = context["ti"]
    dagRun = context["dag_run"]
    with create_session() as session:
        return [(t.task_id, t.state)
                for t in dagRun.get_task_instances(session=session)
                if t.task_id != ti.task_id]


def _streakLen(context, taskId):
    """Длина серии «настоящих» падений линии (RECORD — прозрачны;
    FATAL — заведомо «уже за порогом», возвращаем заведомо большое число)."""
    ti = context["ti"]
    n = 0
    for state, _end, runId, _ed in _taskHistory(context, taskId=taskId):
        if state == State.FAILED:
            cls = _xcomErrorClass(ti.dag_id, taskId, runId)
            if cls == "record":
                continue
            if cls == "fatal":
                return len(FREQUENT_BACKOFF_MIN) + 1   # уже заморожена
            n += 1
        elif state == State.SKIPPED:
            continue
        else:
            break
    return n


def _allLinesFrozen(context, retryMode):
    """True, только если ВСЕ линии исчерпали ретраи (с учётом текущего run).

    RECORD-падение в текущем run НЕ считается за заморозку — линия живая,
    просто конкретные записи провалились. Watcher не удерживает даг.
    """
    ti = context["ti"]
    currentRunId = context["dag_run"].run_id
    lines = _lineStates(context)
    if not lines:
        return False
    for taskId, state in lines:
        if state != State.FAILED:
            return False
        currentCls = _xcomErrorClass(ti.dag_id, taskId, currentRunId)
        if currentCls == "record":
            return False                  # линия жива
        if retryMode == "frequent":
            if currentCls == "fatal":
                continue                  # эта точно заморожена
            if _streakLen(context, taskId) + 1 <= len(FREQUENT_BACKOFF_MIN):
                return False              # есть ещё backoff-попытки
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
    """Сторож после всех линий. Если ВСЕ линии исчерпали ретраи —
    зависает в sleep'е, удерживая DAG-run открытым: max_active_runs=1 не
    даёт плодить новые красные запуски, а DAG остаётся в фильтре активных."""
    watcher = PythonOperator(
        task_id="freeze_watcher",
        provide_context=True,
        python_callable=_freezeWatcher(retryMode),
        trigger_rule="all_done",
    )
    for task in lineTasks:
        task >> watcher
    return watcher
