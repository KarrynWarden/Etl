"""Общая обвязка для DAG'ов: логгер, фабрики задач (runEtl, makeEtlOperator),
FSM-ретраев по тикам расписания, watcher с авто-удержанием при полной
заморозке, ссылки на первую ошибку.

Классы ошибок (записываются в XCom 'error_class' на упавшей задаче):
  - 'record'    — ошибка по конкретной записи: запись припаркована
                   (isetl=-1), линия НЕ морозится, квадрат 🟥 для видимости;
  - 'retryable' — соединение умерло — FSM считает в backoff-серию;
  - 'fatal'     — структурная (Programming, FLK, битый JSON конфига) —
                   FSM сразу frozen, ретраи бессмысленны.

Заморозка линии и пустой запуск визуально одинаково розовые (🟪 skipped),
но различимы по XCom 'frozen=True' — watcher по этому маркеру решает,
удерживать DAG-run или нет.

SIGTERM при перезапуске airflow конвертируется в SkipException (🟪) —
не загрязняет историю ложными красными квадратами при деплое.
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
#               История задачи + XCom-маркеры
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


def _xcom(dagId, taskId, runId, key):
    """Прочитать произвольный XCom-ключ конкретного TI. None если нет."""
    try:
        with create_session() as session:
            return XCom.get_one(
                run_id=runId, task_id=taskId, dag_id=dagId,
                key=key, session=session,
            )
    except Exception:
        return None


def _xcomErrorClass(dagId, taskId, runId):
    return _xcom(dagId, taskId, runId, "error_class")


def _xcomFrozenMark(dagId, taskId, runId):
    """True, если на этом TI стоит маркер 'это skip из-за заморозки'."""
    return bool(_xcom(dagId, taskId, runId, "frozen"))


# ----------------------------------------------------------------------------
#                       FSM ретраев (FREQUENT-режим)
# ----------------------------------------------------------------------------

def _retryDecision(context):
    """Решение FREQUENT-режима. Возвращает (action, info).

    action: 'run'   — выполнять Do_etl;
            'wait'  — ещё рано (skip, не маркируем frozen);
            'frozen'— серия исчерпана или fatal-ошибка.

    Логика серии:
    - RECORD-падения прозрачны (не считаются, серию не рвут).
    - FATAL-падение сразу даёт frozen.
    - RETRYABLE / неклассифицированное — копится в серию по backoff.
    """
    ti = context["ti"]
    failed = []
    for state, endDate, runId, execDate in _taskHistory(context):
        if state == State.FAILED:
            cls = _xcomErrorClass(ti.dag_id, ti.task_id, runId)
            if cls == "record":
                continue
            if cls == "fatal":
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


def _markFrozen(ti, errorClass):
    """Маркеры на TI для заморозки: error_class + явный frozen=True.
    XCom 'frozen' нужен watcher'у, чтобы отличить «skip от заморозки»
    от «skip потому что нет записей»."""
    ti.xcom_push(key="error_class", value=errorClass)
    ti.xcom_push(key="frozen", value=True)


def runEtl(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None,
           retryMode="frequent", **opts):
    """Обёртка ETL-задачи: ретраи + заморозка упавшей линии + XCom-маркеры.

    retryMode='frequent' — кастомный FSM (_retryDecision) по тикам расписания;
    retryMode='rare'     — ретраи делает airflow (экспонента), затем заморозка
                           через _previousRunFailed.

    Заморозка отдаёт 🟪 (AirflowSkipException), но с XCom-меткой frozen=True —
    watcher по этой метке решает, удерживать DAG-run или нет.
    """
    line = tableNameEtlJobs or tableNameMaster

    def _task(**context):
        ti = context["ti"]

        # ---------- FREQUENT: FSM-ретраев ----------
        if retryMode == "frequent":
            action, info = _retryDecision(context)
            if action in ("wait", "frozen"):
                url = _logsUrl(context, info["firstFailRunId"],
                               info["firstFailExecDate"])
                msg = (f"линия {line}, падений {info['failCount']}, "
                       f"backoff {FREQUENT_BACKOFF_MIN} мин")
                if action == "wait":
                    logging.warning("ОЖИДАНИЕ РЕТРАЯ — %s; следующий не "
                                    "раньше %s", msg, info["dueAt"])
                    logging.warning("Первая ошибка: %s", url)
                    raise AirflowSkipException
                why = "FATAL — структурная/SQL-ошибка" \
                    if info.get("errorClass") == "fatal" \
                    else "исчерпаны backoff-ретраи"
                logging.error("ЛИНИЯ ЗАМОРОЖЕНА (%s) — %s; разморозка: "
                              "mark success на упавшей задаче", why, msg)
                logging.error("Первая ошибка: %s", url)
                _markFrozen(ti, info.get("errorClass", "retryable"))
                raise AirflowSkipException(f"Линия {line} заморожена")
            logging.info("ЗАПУСК ETL — линия %s, попытка %s",
                         line, info["attempt"])
        else:
            # ---------- RARE: airflow-ретраи + 1-shot freeze ----------
            failed, firstRunId, firstExecDate = _previousRunFailed(context)
            if failed:
                url = _logsUrl(context, firstRunId, firstExecDate)
                logging.error("ЛИНИЯ ЗАМОРОЖЕНА — линия %s; разморозка: "
                              "mark success на упавшей задаче", line)
                logging.error("Первая ошибка: %s", url)
                _markFrozen(ti, "retryable")
                raise AirflowSkipException(f"Линия {line} заморожена")

        # ---------- Сам ETL + классификация результата ----------
        try:
            Do_etl(tableNameMaster=tableNameMaster, dbMaster=dbMaster,
                   dbSlave=dbSlave, tableNameEtlJobs=tableNameEtlJobs, **opts)
        except AirflowSkipException:
            # «нет записей для обновления» — пробрасываем как обычный skip,
            # БЕЗ frozen-маркера → watcher видит «линия жива».
            raise
        except RecordScopeError as err:
            # часть записей не перенесена — линию НЕ морозим, квадрат 🟥
            ti.xcom_push(key="error_class", value="record")
            logging.error("RECORD-ошибки в запуске линии %s: %s", line, err)
            raise AirflowException(f"Линия {line}: {err}")
        except AirflowFailException as err:
            # FLK или иное мгновенно-фатальное (raise из Do_etl)
            ti.xcom_push(key="error_class", value="fatal")
            logging.error("FATAL в линии %s: %s", line, err)
            raise
        except Exception as err:
            # SIGTERM при перезапуске airflow — это не ошибка ETL, помечаем
            # skip (без frozen-метки), чтобы деплой не плодил красных квадратов
            if _isSigterm(err):
                logging.warning("Линия %s: получен SIGTERM (перезапуск "
                                "airflow) — помечаю как skip", line)
                raise AirflowSkipException("Прервано SIGTERM")
            # Дефолт неклассифицированного снаружи per-record loop — fatal:
            # это системные вещи (битый JSON конфига, ImportError и т.п.),
            # ретраи их не починят, нужен человек.
            cls = classifyError(err) or "fatal"
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
    """Простой PythonOperator без ETL-обвязки."""
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
                    triggerRule=None, pool="Test", **opts):
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
    """Длина серии «настоящих» падений линии (RECORD прозрачны;
    FATAL → заведомо «уже за порогом»)."""
    ti = context["ti"]
    n = 0
    for state, _end, runId, _ed in _taskHistory(context, taskId=taskId):
        if state == State.FAILED:
            cls = _xcomErrorClass(ti.dag_id, taskId, runId)
            if cls == "record":
                continue
            if cls == "fatal":
                return len(FREQUENT_BACKOFF_MIN) + 1
            n += 1
        elif state == State.SKIPPED:
            continue
        else:
            break
    return n


def _isLineFrozenNow(context, taskId, state, retryMode):
    """Заморожена ли линия по итогам ТЕКУЩЕГО run'а?

    SKIPPED + XCom frozen=True   → ДА (заморожена FSM в runEtl).
    SKIPPED без frozen-маркера   → НЕТ (это «нет записей для обновления»).
    SUCCESS / RUNNING / прочее   → НЕТ.
    FAILED + error_class=record  → НЕТ (отдельные записи провалены, линия жива).
    FAILED + error_class=fatal   → ДА (заморозится на следующем тике в любом случае).
    FAILED (retryable/unknown):
       - в frequent: ДА только если backoff-серия исчерпана с учётом текущего;
       - в rare: ДА (один failed = retries уже исчерпаны внутри run'а).
    """
    ti = context["ti"]
    currentRunId = context["dag_run"].run_id
    if state == State.SKIPPED:
        return _xcomFrozenMark(ti.dag_id, taskId, currentRunId)
    if state != State.FAILED:
        return False
    cls = _xcomErrorClass(ti.dag_id, taskId, currentRunId)
    if cls == "record":
        return False
    if cls == "fatal":
        return True
    if retryMode == "rare":
        return True
    # frequent: учитываем backoff-серию + текущий failed
    return _streakLen(context, taskId) + 1 > len(FREQUENT_BACKOFF_MIN)


def _allLinesFrozen(context, retryMode):
    """True, только если ВСЕ линии заморожены (по правилам _isLineFrozenNow).

    «Пустые» SKIPPED-линии без frozen-маркера трактуются как живые → даг
    не удерживается, продолжает работать в штатном режиме.
    """
    lines = _lineStates(context)
    if not lines:
        return False
    for taskId, state in lines:
        if not _isLineFrozenNow(context, taskId, state, retryMode):
            return False
    return True


def _hasAnyFailed(context):
    """Есть ли в текущем run'е хотя бы один FAILED (для сводного 🟥)."""
    return any(st == State.FAILED for _, st in _lineStates(context))


def _freezeWatcher(retryMode):
    def _watch(**context):
        # 1) все линии заморожены → удерживаем DAG-run (бесконечный даг)
        if _allLinesFrozen(context, retryMode):
            logging.error("Watcher: ВСЕ линии заморожены. Удерживаю DAG-run "
                          "(новые запуски не плодятся). Разморозка — mark "
                          "success на упавших/замороженных линиях этого "
                          "запуска.")
            while _allLinesFrozen(context, retryMode):
                time.sleep(WATCHER_RECHECK_SEC)
            logging.info("Watcher: линии разморожены — отпускаю DAG-run.")
            return
        # 2) есть упавшие → сводный статус run'а 🟥
        if _hasAnyFailed(context):
            failed = [tid for tid, st in _lineStates(context)
                      if st == State.FAILED]
            raise AirflowException(
                f"В запуске есть упавшие линии: {', '.join(failed)}. "
                f"Сводный статус — ошибка; ретраи и причина — в самих линиях."
            )
        logging.info("Watcher: упавших линий нет — даг зелёный.")
    return _watch


def addFreezeWatcher(lineTasks, retryMode="frequent"):
    """Сторож после всех линий. Если ВСЕ линии заморожены — зависает в
    sleep'е, удерживая DAG-run открытым: max_active_runs=1 не даёт плодить
    новые квадраты, а DAG остаётся в фильтре активных."""
    watcher = PythonOperator(
        task_id="freeze_watcher",
        provide_context=True,
        python_callable=_freezeWatcher(retryMode),
        trigger_rule="all_done",
    )
    for task in lineTasks:
        task >> watcher
    return watcher
