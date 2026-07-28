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
import json
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
from Functions.do_audit import Do_audit, AuditScopeError
from Src.fullPath import FULL_PATH, WEB_BASE_URL


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

WATCHER_MAX_RETRIES = 10000
WATCHER_RETRY_DELAY_SEC = 30

# Пул ETL-переносов и его полный размер. Задача-замок etl_lock в аудит-DAG
# занимает ВЕСЬ этот пул на время прогона аудита, поэтому ETL-переносы
# (pool_slots=1) на это время не стартуют. САМИ аудит-линии идут в другом
# пуле (default_pool) и потому выполняются ПАРАЛЛЕЛЬНО друг другу.
# freeze_watcher тоже в default_pool, поэтому замороженные ETL-даги слоты
# ETL-пула не держат и взять замок не мешают.
# ВАЖНО: ETL_POOL_SLOTS не должен превышать реальный размер пула, иначе замок
# никогда не получит все слоты (дедлок). Пул "Etl" = 100 слотов.
ETL_POOL = "Etl"
ETL_POOL_SLOTS = 100

# Задача-замок: id, XCom-сигнал «пул занят», таймаут ожидания сигнала аудитом.
ETL_LOCK_TASK_ID = "etl_lock"
ETL_LOCK_XCOM_KEY = "etl_locked"
ETL_LOCK_WAIT_TIMEOUT_SEC = 600

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


def _retryGate(context, line, retryMode, label):
    """Общая преамбула ретраев/заморозки для ETL- и Audit-линий.

    Возвращает управление, если линию нужно выполнять; иначе сама бросает
    AirflowSkipException (ожидание ретрая или заморозка с frozen-меткой).
    label — что писать в лог при старте ('ETL' / 'AUDIT')."""
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
        logging.info("ЗАПУСК %s — линия %s, попытка %s",
                     label, line, info["attempt"])
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


def runEtl(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None,
           retryMode="frequent", **opts):
    """Обёртка ETL-задачи: ретраи + заморозка упавшей линии + XCom-маркеры.

    retryMode='frequent' — кастомный FSM (_retryDecision) по тикам расписания;
    retryMode='rare'     — ретраи делает airflow (экспонента), затем заморозка
                           через _previousRunFailed.

    Заморозка отдаёт 🟪 (AirflowSkipException), но с XCom-меткой frozen=True —
    watcher по этой метке решает, удерживать DAG-run или нет.

    РЕТРАИТСЯ ТОЛЬКО КЛАСС retryable (мёртвое соединение). Итоговая раскладка:

    | что случилось              | error_class | ретраи | квадрат | заморозка |
    |----------------------------|-------------|--------|---------|-----------|
    | нечего переносить          | —           | нет    | ⬜      | нет       |
    | SIGTERM (рестарт airflow)  | —           | нет    | ⬜      | нет       |
    | соединение умерло          | retryable   | ДА     | 🟡→🟥   | да        |
    | плохая запись / констрейнт | record      | нет    | 🟥      | нет       |
    | битый SQL / структура (FLK)| fatal       | нет    | 🟥      | да        |
    """
    line = tableNameEtlJobs or tableNameMaster

    def _task(**context):
        ti = context["ti"]
        _retryGate(context, line, retryMode, "ETL")

        # ---------- Сам ETL + классификация результата ----------
        try:
            Do_etl(tableNameMaster=tableNameMaster, dbMaster=dbMaster,
                   dbSlave=dbSlave, tableNameEtlJobs=tableNameEtlJobs, **opts)
        except AirflowSkipException:
            # «нет записей для обновления» — пробрасываем как обычный skip,
            # БЕЗ frozen-маркера → watcher видит «линия жива».
            raise
        except RecordScopeError as err:
            # Часть записей не перенесена (плохие данные / констрейнт). Линию НЕ
            # морозим (error_class='record' прозрачен для FSM и watcher'а), но
            # ретраить НЕЛЬЗЯ: припаркованные записи уже помечены isetl=-1 и
            # повторный запуск их не выбирает — он находит пустой журнал и
            # завершается skip'ом. Красный квадрат при этом перекрашивался в
            # ⬜, и в общем списке дагов ошибка становилась не видна.
            # AirflowFailException пропускает оставшиеся retries: запуск
            # остаётся 🟥 ровно там, где произошла ошибка. Так же поступает
            # аудит с AuditScopeError.
            ti.xcom_push(key="error_class", value="record")
            logging.error("RECORD-ошибки в запуске линии %s: %s. Авто-ретрая "
                          "не будет — записи припаркованы (isetl=-1), нужен "
                          "разбор человеком.", line, err)
            raise AirflowFailException(f"Линия {line}: {err}")
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
            # Ретраи имеют смысл ТОЛЬКО для retryable (мёртвое соединение) —
            # там повтор действительно может пройти. record (плохая запись) и
            # fatal (битый SQL/структура) повтором не чинятся: record ещё и
            # перекрашивал 🟥 в ⬜, потому что припаркованные записи на второй
            # попытке уже не выбираются. Поэтому всё, кроме retryable, —
            # AirflowFailException: терминальный 🟥 без ретраев.
            if cls == "retryable":
                raise AirflowException(f"Линия {line}: {err}")
            raise AirflowFailException(f"Линия {line}: {err}")

    return _task


# ----------------------------------------------------------------------------
#                       Обёртка задачи: runAudit
# ----------------------------------------------------------------------------

def runAudit(tableNameMaster, dbMaster, dbSlave, tableNameEtlJobs=None,
             retryMode="rare", **opts):
    """Обёртка Audit-задачи: та же машина ретраев/заморозки, что у runEtl,
    но вызывает Do_audit. RecordScopeError у аудита нет — он коммитит статус
    по каждой группе сам (см. do_audit._auditGroup), поэтому SIGTERM на
    середине просто прерывает текущую группу, а остаток доберёт следующий
    запуск (isokaudit != 1)."""
    line = tableNameEtlJobs or tableNameMaster

    def _task(**context):
        ti = context["ti"]
        _retryGate(context, line, retryMode, "AUDIT")
        # Не трогаем БД, пока замок не занял пул ETL (иначе аудит мог бы
        # стартовать параллельно с переносом и принять недоношенные данные).
        _waitEtlLock(context)

        try:
            Do_audit(tableNameMaster=tableNameMaster, dbMaster=dbMaster,
                     dbSlave=dbSlave, tableNameEtlJobs=tableNameEtlJobs, **opts)
        except AirflowSkipException:
            # «нет групп для проверки» — обычный skip без frozen-метки.
            raise
        except AuditScopeError as err:
            # не все группы идентичны (-4/-2): красим 🟥 для видимости, но
            # БЕЗ авто-ретраев и БЕЗ заморозки. Ретрай аудита почти никогда не
            # помогает (если в переносе реальная ошибка — она повторится), а
            # 6 ретраев по несколько часов зря грузят железо. Поэтому
            # AirflowFailException (пропускает оставшиеся retries), а
            # error_class='record' — чтобы FSM/watcher не морозили линию.
            # Статусы по группам уже в etl_jobs; человек смотрит и решает,
            # перезапускать аудит/перенос вручную или нет.
            ti.xcom_push(key="error_class", value="record")
            logging.error("Аудит линии %s: %s. Авто-ретрая не будет — "
                          "нужен разбор человеком (см. etl_jobs.isokaudit).",
                          line, err)
            raise AirflowFailException(f"Аудит {line}: {err}")
        except AirflowFailException as err:
            ti.xcom_push(key="error_class", value="fatal")
            logging.error("FATAL в аудите линии %s: %s", line, err)
            raise
        except Exception as err:
            if _isSigterm(err):
                logging.warning("Аудит %s: получен SIGTERM — помечаю как skip "
                                "(проверенные группы уже закоммичены)", line)
                raise AirflowSkipException("Прервано SIGTERM")
            cls = classifyError(err) or "fatal"
            ti.xcom_push(key="error_class", value=cls)
            logging.error("Аудит %s (класс=%s): %s", line, cls, err)
            if cls == "fatal":
                raise AirflowFailException(f"Аудит {line}: {err}")
            raise AirflowException(f"Аудит {line}: {err}")

    return _task


# ----------------------------------------------------------------------------
#                       Сборка PythonOperator
# ----------------------------------------------------------------------------

def buildOperator(taskId, callable_, triggerRule=None):
    """Простой PythonOperator без ETL-обвязки."""
    kwargs = dict(
        task_id=taskId,
        pool="Etl",
        provide_context=True,
        priority_weight=1,
        python_callable=callable_,
    )
    if triggerRule is not None:
        kwargs["trigger_rule"] = triggerRule
    return PythonOperator(**kwargs)


def lineEnabled(tableNameEtlJobs, dbMaster, dbSlave):
    """Включена ли линия (нет флага `disabled` в её конфиге).

    Нужна дагам-ИТЕРАТОРАМ (MocheckOrclPost и т.п.), которые перечисляют
    несколько линий в одном файле: у таких линий нет своего файла дага, поэтому
    «архивировать» их переносом файла нельзя — конструктор ставит `disabled`, а
    даг обязан такую линию пропустить. Ошибка чтения конфига НЕ должна ронять
    парсинг дага, поэтому при любой проблеме считаем линию включённой.
    """
    try:
        from Functions.functionsFile.loadConfig import loadFullConfig
        body = loadFullConfig()["data"].get(
            f"{tableNameEtlJobs}{dbMaster}{dbSlave}", {})
        return not body.get("disabled")
    except Exception:
        return True


def makeEtlOperator(taskId, tableNameMaster, dbMaster, dbSlave,
                    tableNameEtlJobs=None, retryMode="frequent",
                    triggerRule=None, pool="Etl", **opts):
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


def makeAuditOperator(taskId, tableNameMaster, dbMaster, dbSlave,
                      tableNameEtlJobs=None, retryMode="rare",
                      triggerRule=None, pool="default_pool", **opts):
    """Собрать PythonOperator аудит-линии (callable runAudit + ретраи).

    По умолчанию retryMode='rare': аудит запускается редко (в конце дня),
    ретраи внутри дня делает airflow, заморозка — после исчерпания.

    Аудит-линии живут в default_pool (НЕ в пуле ETL) и потому параллельны
    между собой. Блокировку ETL на время прогона даёт отдельная задача-замок
    etl_lock (см. addEtlLock), а не занятость слотов каждой линией.
    """
    kwargs = dict(
        task_id=taskId,
        pool=pool,
        provide_context=True,
        priority_weight=1,
        python_callable=runAudit(tableNameMaster, dbMaster, dbSlave,
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
#                Перечисление линий из config.json (для общего AuditDag)
# ----------------------------------------------------------------------------


def iterAuditLines():
    """Все линии переноса из config.json — по одной на каждый ключ.

    Ключ конфига = tableNameEtlJobs + dbMaster + dbSlave, где последние 8
    символов — два имени СУБД по 4 буквы (Post/Orcl). Отсюда разбираем
    направление и имя группы; tableNameMaster берём из самой записи
    (или = префиксу ключа). Возвращает список словарей, готовых к передаче
    в makeAuditOperator.
    """
    from Functions.functionsFile.loadConfig import loadFullConfig
    data = loadFullConfig()["data"]

    lines = []
    for key, entry in data.items():
        if not isinstance(entry, dict):
            continue

        # ⬇️ НОВОЕ: Пропускаем таблицы, для которых аудит временно отключен
        if entry.get("skipAudit"):
            logging.info("AuditDag: пропускаю ключ '%s' (skipAudit=true)", key)
            continue
        dbMaster, dbSlave = key[-8:-4], key[-4:]
        if dbMaster not in ("Post", "Orcl") or dbSlave not in ("Post", "Orcl"):
            logging.warning("AuditDag: пропускаю ключ '%s' — не распознано "
                            "направление", key)
            continue
        tableNameEtlJobs = key[:-8]
        tableNameMaster = entry.get("tableNameMaster", tableNameEtlJobs)
        lines.append({
            "key": key,
            "tableNameMaster": tableNameMaster,
            "dbMaster": dbMaster,
            "dbSlave": dbSlave,
            "tableNameEtlJobs": tableNameEtlJobs,
        })
    return lines

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
        # 2) есть упавшие → сводный статус run'а 🟥.
        # AirflowFailException (а не AirflowException): это терминальный 🟥, его
        # НЕ нужно ретраить — иначе с большим WATCHER_MAX_RETRIES он бы крутился.
        # SIGTERM же прилетает обычным AirflowException и уходит в ретрай (hold
        # переживает рестарт).
        if _hasAnyFailed(context):
            failed = [tid for tid, st in _lineStates(context)
                      if st == State.FAILED]
            raise AirflowFailException(
                f"В запуске есть упавшие линии: {', '.join(failed)}. "
                f"Сводный статус — ошибка; ретраи и причина — в самих линиях."
            )
        logging.info("Watcher: упавших линий нет — даг зелёный.")
    return _watch


# ----------------------------------------------------------------------------
#         Замок ETL на время аудита (ETL стоит, аудит-линии параллельны)
# ----------------------------------------------------------------------------

def _tasksTerminal(context, taskIds):
    """True, если ВСЕ задачи taskIds в этом dag-run завершены (терминальны).

    Перечитывает состояния из БД каждый вызов (как _lineStates) — иначе в
    долгоживущем замке состояние было бы закэшировано на старте."""
    dagRun = context["dag_run"]
    wanted = set(taskIds)
    terminal = {State.SUCCESS, State.FAILED, State.SKIPPED,
                State.UPSTREAM_FAILED}
    with create_session() as session:
        states = {t.task_id: t.state
                  for t in dagRun.get_task_instances(session=session)
                  if t.task_id in wanted}
    for tid in wanted:
        if states.get(tid) not in terminal:
            return False
    return True


def _etlLock(auditTaskIds):
    """Задача-замок: держит ВЕСЬ пул ETL, пока идут аудит-линии этого dag-run.

    Зависимостей с аудит-линиями НЕ имеет — стартует параллельно, забирает все
    слоты пула ETL (priority_weight выше ETL-задач, чтобы получить слоты
    раньше) и опрашивает завершение линий. Пока замок жив — ETL-переносы
    (pool_slots=1 в том же пуле) не стартуют, а сами аудит-линии в default_pool
    идут параллельно.
    """
    def _lock(**context):
        ti = context["ti"]
        ti.xcom_push(key=ETL_LOCK_XCOM_KEY, value=1)
        logging.info("ETL-замок взят: держу пул %s (%d слотов), пока идёт "
                     "аудит (%d линий).", ETL_POOL, ETL_POOL_SLOTS,
                     len(auditTaskIds))
        while not _tasksTerminal(context, auditTaskIds):
            time.sleep(WATCHER_RECHECK_SEC)
        logging.info("ETL-замок снят: аудит-линии завершены, отпускаю пул %s.",
                     ETL_POOL)
    return _lock


def _waitEtlLock(context):
    """Подождать XCom-сигнал от задачи-замка, что пул ETL занят.

    Если замка в DAG нет — гейт не используется, идём сразу. Если замок упал/
    пропущен или сигнал не пришёл за таймаут — идём без гейта (лучше прогнать
    аудит, чем зависнуть), залогировав предупреждение.
    """
    ti = context["ti"]
    dagRun = context["dag_run"]
    if dagRun.get_task_instance(ETL_LOCK_TASK_ID) is None:
        return
    waited = 0
    while waited < ETL_LOCK_WAIT_TIMEOUT_SEC:
        if ti.xcom_pull(task_ids=ETL_LOCK_TASK_ID, key=ETL_LOCK_XCOM_KEY):
            return
        lockTi = dagRun.get_task_instance(ETL_LOCK_TASK_ID)
        if lockTi is not None and lockTi.state in (
            State.FAILED, State.SKIPPED, State.UPSTREAM_FAILED,
        ):
            logging.warning("ETL-замок не взят (%s) — аудит идёт без гейта.",
                            lockTi.state)
            return
        time.sleep(5)
        waited += 5
    logging.warning("ETL-замок не подтверждён за %d с — аудит идёт без гейта.",
                    ETL_LOCK_WAIT_TIMEOUT_SEC)


def addEtlLock(auditTasks):
    """Добавить в аудит-DAG задачу-замок, занимающую весь пул ETL на время
    прогона. Вызывать внутри `with DAG(...)`. Возвращает задачу-замок."""
    return PythonOperator(
        task_id=ETL_LOCK_TASK_ID,
        pool=ETL_POOL,
        pool_slots=ETL_POOL_SLOTS,
        priority_weight=10_000,  # забрать слоты пула раньше ETL-задач
        provide_context=True,
        python_callable=_etlLock([t.task_id for t in auditTasks]),
        retries=0,
    )


def addFreezeWatcher(lineTasks, retryMode="frequent"):
    """Сторож после всех линий. Если ВСЕ линии заморожены — зависает в
    sleep'е, удерживая DAG-run открытым: max_active_runs=1 не даёт плодить
    новые квадраты, а DAG остаётся в фильтре активных."""
    watcher = PythonOperator(
        task_id="freeze_watcher",
        provide_context=True,
        python_callable=_freezeWatcher(retryMode),
        trigger_rule="all_done",
        retries=WATCHER_MAX_RETRIES,
        retry_delay=dt.timedelta(seconds=WATCHER_RETRY_DELAY_SEC),
        retry_exponential_backoff=False,
    )
    for task in lineTasks:
        task >> watcher
    return watcher







