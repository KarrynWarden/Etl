"""DAG: продление ЭИНДЕКСов по журналу заявок Oracle.

Раньше это была цепочка из двух шагов: линия LogeindprolongOrclPost возила
koknaev.LOG_EINDPROLONG с Oracle на Postgres, а процедура
pck_eindexmo.scanlogprolong раз в час разбирала уже перенесённую копию. Копия
на Postgres нужна была только процедуре, поэтому перенос убран, а логика
курсора scanlogprolong переехала сюда:

    1. SELECT на Oracle — открытые заявки (dwork is null), по одной свежей на
       ключ МО+отдел+подразделение+подподразделение;
    2. на каждую заявку — CALL pck_eindexmo.ProlongDestrEindex(...) на Postgres
       с параметрами из Oracle;
    3. результат вызова пишется обратно в Oracle: dwork/lastupdate = sysdate,
       status = 'OK' | 'ERR'.

Заявки независимы: падение одной не прекращает разбор остальных (как
`continue` в блоке EXCEPTION прежней процедуры). Упавшая заявка получает
status='ERR' и строку в public.ias_func_errlog (idfunc = 286) — ровно как
раньше, и точно так же НЕ повторяется автоматически: продление ЭЦП не то
действие, которое можно вслепую переиграть, нужен разбор человеком.

Запуск идёт в пуле Etl (pool_slots=1), поэтому на время аудита (задача-замок
etl_lock занимает весь пул) даг не стартует: процедура правит public.eindexmo,
а это ведущая таблица линии eindexmo PG→Oracle — иначе аудит мог бы поймать
её на середине правки.
"""
import datetime as dt
import logging
import traceback

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import DAG

from Connect import DbConnectOrcl, DbConnectPost
from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH

_QUERIES = f"{FULL_PATH}etlFolder/queries/procedures/scanLogProlong/"

selectPendingOrclSql = TakeOneQuery(f"{_QUERIES}SelectPendingOrcl.sql")
prolongDestrEindexPostSql = TakeOneQuery(f"{_QUERIES}ProlongDestrEindexPost.sql")
markProcessedOrclSql = TakeOneQuery(f"{_QUERIES}MarkProcessedOrcl.sql")
addErrLogPostSql = TakeOneQuery(f"{_QUERIES}AddErrLogPost.sql")

# Что писать в errtype: раньше там стояло 'from proc PCK_EINDEXMO.SCANLOGPROLONG'.
ERR_TYPE = "from dag A61ProceduresScanLogProlong"

# Ограничения на длину текста ошибки — колонки журнала ошибок ограничены,
# а трассировка python длиннее любого SQLERRM.
ERR_TEXT_LIMIT = 2000
PARAM_IN_LIMIT = 2000
STACKTRACE_LIMIT = 4000

args = {**DEFAULT_ARGS, "retries": 2, "retry_delay": dt.timedelta(minutes=5)}


def _asInt(value):
    """NUMBER Oracle -> int (аналог rec.mo::integer). NULL остаётся NULL."""
    return None if value is None else int(value)


def _asDateString(value):
    """DATE Oracle -> 'dd.mm.yyyy' (аналог to_char(destrdate,'dd.mm.yyyy'))."""
    return None if value is None else value.strftime("%d.%m.%Y")


def _describe(params):
    """Параметры вызова строкой — в лог задачи и в param_in журнала ошибок."""
    mo, otdel, dept, subdept, destrdate = params
    return (f"mo={mo}, otdel={otdel}, dept={dept}, subdept={subdept}, "
            f"destrdate={destrdate}")


def _addErrLog(post, postCursor, params, error):
    """Записать ошибку вызова процедуры в public.ias_func_errlog.

    Сам журнал ошибок — вещь вспомогательная: если запись в него не удалась
    (нет прав, таблицы, места), это не повод потерять разбор остальных заявок.
    """
    try:
        postCursor.execute(addErrLogPostSql, (
            ERR_TYPE,
            str(error)[:ERR_TEXT_LIMIT],
            _describe(params)[:PARAM_IN_LIMIT],
            traceback.format_exc()[:STACKTRACE_LIMIT],
        ))
        post.commit()
    except Exception as logError:
        post.rollback()
        logging.error("Не удалось записать ошибку в public.ias_func_errlog: %s",
                      logError)


def _safeMark(orcl, orclCursor, key, status, failed):
    """Пометить заявку в Oracle ('OK' | 'ERR'), не роняя разбор остальных.

    Если пометка не прошла, заявка останется открытой и уедет в следующий
    запуск (для 'OK' это значит повторное продление — поэтому пишем ошибку
    громко), но остальные заявки текущего запуска обработать надо.
    """
    try:
        orclCursor.execute(markProcessedOrclSql, {**key, "status": status})
        orcl.commit()
        return True
    except Exception as error:
        orcl.rollback()
        logging.error("Не удалось проставить status='%s' в LOG_EINDPROLONG "
                      "(mo=%s, otdel=%s, dept=%s, subdept=%s): %s — заявка "
                      "останется открытой и попадёт в следующий запуск",
                      status, key["mo"], key["otdel"], key["dept"],
                      key["subdept"], error)
        failed.append(f"пометка {status} не прошла: "
                      f"mo={key['mo']}, otdel={key['otdel']}, "
                      f"dept={key['dept']}, subdept={key['subdept']}")
        return False


def _closeCursor(cursor):
    if cursor is None:
        return
    try:
        cursor.close()
    except Exception as error:
        logging.warning("не удалось закрыть курсор: %s", error)


def _close(connection, name):
    if connection is None:
        logging.info("соединение %s не было открыто", name)
        return
    try:
        connection.close()
        logging.info("соединение %s закрыто", name)
    except Exception as error:
        logging.warning("не удалось закрыть соединение %s: %s", name, error)


def scanLogProlong(**context):
    orcl = None
    post = None
    orclCursor = None
    postCursor = None
    okCount = 0
    failed = []
    try:
        orcl = DbConnectOrcl()
        post = DbConnectPost()
        orclCursor = orcl.cursor()
        postCursor = post.cursor()

        orclCursor.execute(selectPendingOrclSql)
        rows = orclCursor.fetchall()

        if not rows:
            logging.info("В LOG_EINDPROLONG нет заявок с dwork is null — "
                         "продлевать нечего.")
            raise AirflowSkipException("Нет заявок на продление")

        logging.info("Заявок на продление: %d", len(rows))

        for mo, otdel, dept, subdept, destrdate, timeoper in rows:
            key = {"mo": _asInt(mo), "otdel": _asInt(otdel),
                   "dept": _asInt(dept), "subdept": _asInt(subdept)}
            params = (key["mo"], key["otdel"], key["dept"], key["subdept"],
                      _asDateString(destrdate))

            # 1) процедура на Postgres
            try:
                postCursor.execute(prolongDestrEindexPostSql, params)
                post.commit()
            except Exception as error:
                # Транзакцию Postgres надо откатить обязательно: после ошибки
                # соединение до ROLLBACK не примет ни одной команды, и вызов
                # для следующей заявки упал бы уже на пустом месте.
                post.rollback()
                logging.error("ProlongDestrEindex(%s): %s",
                              _describe(params), error)
                _addErrLog(post, postCursor, params, error)
                failed.append(_describe(params))
                _safeMark(orcl, orclCursor, key, "ERR", failed)
                continue

            # 2) итог обратно в журнал Oracle
            if _safeMark(orcl, orclCursor, key, "OK", failed):
                okCount += 1
                logging.info("Продлено: %s", _describe(params))
    finally:
        _closeCursor(postCursor)
        _closeCursor(orclCursor)
        _close(post, "Postgres")
        _close(orcl, "Oracle")

    logging.info("Итог: продлено %d, с ошибкой %d", okCount, len(failed))
    if failed:
        # AirflowFailException, а не AirflowException: ретраить нечего —
        # упавшие заявки уже помечены 'ERR' и в выборку больше не попадут,
        # повторный запуск нашёл бы пустой журнал и перекрасил 🟥 в 🟪, спрятав
        # ошибку. Пусть запуск остаётся красным там, где она случилась.
        raise AirflowFailException(
            f"Заявок с ошибкой: {len(failed)} (см. public.ias_func_errlog, "
            f"idfunc = 286): {'; '.join(failed)}")


with DAG(
    dag_id="A61ProceduresScanLogProlong",
    default_args=args,
    max_active_runs=1,
    tags=["A61", "procedures", "DbSync"],
    schedule_interval=dt.timedelta(minutes=60),
    catchup=False,
) as dag:
    configureLogger()
    buildOperator("do_etl_procedures", scanLogProlong)
