"""DAG для ручного размораживания всех активных DAG'ов в PROD окружении после исправления
системной ошибки.

Сценарий использования:
1. Происходит системная ошибка (битый JSON конфига, ImportError и т.п.)
2. Все активные DAG'и замораживаются watcher'ом (error_class='fatal')
3. Вы исправляете ошибку
4. Запускаете этот DAG вручную
5. Он находит все активные запуски с fatal-ошибкой в пуле Prod и помечает их как успешные
6. Watcher видит размороженные линии и отпускает DAG'ы

Важно:
- Обрабатываются ТОЛЬКО активные запуски (state=running) в пуле Prod
- Только те, где ВСЕ линии имеют error_class='fatal' в последнем выполнении
- Не затрагивает record и retryable ошибки — они требуют индивидуального разбора
- Работает ТОЛЬКО с DAG'ами в пулу Prod
"""
import datetime as dt
import logging

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance, XCom
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from sqlalchemy import and_, or_

from Functions._dagHelpers import configureLogger


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 10, 23),
    "timezone": "Asia/Yekaterinburg",
}

TARGET_POOL = "Prod"


def _getActiveDagRunWithFatalErrors():
    """Найти все активные DAG runs в пуле Prod, где все линии заморожены из-за fatal ошибки.

    Возвращает список кортежей (dag_id, run_id) для обработки.

    Критерии отбора:
    1. dag_run.state = 'running' (активный запуск)
    2. Исключаем сам DAG 'unfreeze_all_dags_prod' (рекурсия)
    3. Все задачи в запуске используют пул 'Prod'
    4. Все задачи в запуске (кроме freeze_watcher) имеют:
       - state = 'skipped' с XCom frozen=True И error_class='fatal'
       - ИЛИ state = 'failed' с error_class='fatal'

    Примечание: freeze_watcher исключается, так как он ещё работает
    """
    result = []

    with create_session() as session:
        # Находим все активные запуски
        active_runs = (
            session.query(DagRun.dag_id, DagRun.run_id)
            .filter(
                DagRun.state == State.RUNNING,
                DagRun.dag_id != 'unfreeze_all_dags_prod',  # исключаем себя
            )
            .all()
        )

        logging.info("Найдено активных запусков: %d", len(active_runs))

        for dag_id, run_id in active_runs:
            # Получаем все задачи этого запуска
            tasks = (
                session.query(TaskInstance.task_id, TaskInstance.state, TaskInstance.pool)
                .filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == run_id,
                    TaskInstance.task_id != 'freeze_watcher',  # исключаем watcher
                )
                .all()
            )

            if not tasks:
                # Нет задач — пропускаем
                continue

            # Проверяем, что хотя бы одна задача использует целевой пул
            pool_tasks = [t for t in tasks if t[2] == TARGET_POOL]
            if not pool_tasks:
                logging.debug("DAG %s (run=%s): нет задач в пулу %s, пропускаем", dag_id, run_id, TARGET_POOL)
                continue

            all_fatal = True
            fatal_tasks = []

            for task_id, state, pool in pool_tasks:
                # Проверяем error_class для этой задачи
                xcom_record = (
                    session.query(XCom)
                    .filter(
                        XCom.dag_id == dag_id,
                        XCom.task_id == task_id,
                        XCom.run_id == run_id,
                        XCom.key == 'error_class',
                    )
                    .first()
                )

                error_class = None
                if xcom_record and xcom_record.value is not None:
                    # cx_Oracle/psycopg2 могут возвращать value в разной форме
                    try:
                        import json
                        error_class = json.loads(xcom_record.value) if isinstance(xcom_record.value, str) else xcom_record.value
                    except Exception:
                        error_class = str(xcom_record.value) if xcom_record.value else None

                # Проверяем frozen маркер
                frozen_record = (
                    session.query(XCom)
                    .filter(
                        XCom.dag_id == dag_id,
                        XCom.task_id == task_id,
                        XCom.run_id == run_id,
                        XCom.key == 'frozen',
                    )
                    .first()
                )
                is_frozen = bool(frozen_record and frozen_record.value)

                # Линия считается fatal-замороженной если:
                # 1. state = skipped И frozen=True И error_class='fatal'
                # 2. state = failed И error_class='fatal'
                is_fatal_frozen = False

                if state == State.SKIPPED and is_frozen and error_class == 'fatal':
                    is_fatal_frozen = True
                elif state == State.FAILED and error_class == 'fatal':
                    is_fatal_frozen = True

                if not is_fatal_frozen:
                    all_fatal = False
                    logging.debug(
                        "Задача %s.%s (run=%s) не является fatal-замороженной: "
                        "state=%s, frozen=%s, error_class=%s",
                        dag_id, task_id, run_id, state, is_frozen, error_class
                    )
                else:
                    fatal_tasks.append(task_id)

            if all_fatal and fatal_tasks:
                logging.info(
                    "DAG %s (run=%s): все %d линии в пулу %s заморожены с fatal ошибкой",
                    dag_id, run_id, len(fatal_tasks), TARGET_POOL
                )
                result.append((dag_id, run_id))
            elif fatal_tasks:
                logging.debug(
                    "DAG %s (run=%s): частично заморожен (%d/%d линий fatal), пропускаем",
                    dag_id, run_id, len(fatal_tasks), len(pool_tasks)
                )

    return result


def _markDagRunsAsSuccess(**context):
    """Пометить найденные DAG runs как успешные."""
    ti = context["ti"]

    # Находим кандидаты на разморозку
    candidates = _getActiveDagRunWithFatalErrors()

    if not candidates:
        logging.info("Нет активных DAG'ов в пулу %s с fatal-ошибками для разморозки", TARGET_POOL)
        ti.xcom_push(key="unfrozen_count", value=0)
        ti.xcom_push(key="unfrozen_dags", value=[])
        return

    logging.info("Найдено %d DAG'ов в пулу %s для разморозки", len(candidates), TARGET_POOL)

    unfrozen_dags = []
    success_count = 0
    fail_count = 0

    with create_session() as session:
        for dag_id, run_id in candidates:
            try:
                # Получаем dag_run
                dag_run = (
                    session.query(DagRun)
                    .filter(
                        DagRun.dag_id == dag_id,
                        DagRun.run_id == run_id,
                    )
                    .first()
                )

                if not dag_run:
                    logging.warning("DAG run %s.%s не найден, пропускаем", dag_id, run_id)
                    fail_count += 1
                    continue

                # Помечаем все задачи в целевом пуле как успешные.
                # freeze_watcher работает в default_pool (pool не задан), поэтому
                # включаем его явно — иначе он остаётся 🟥 и красит весь DAG-run.
                tasks = (
                    session.query(TaskInstance)
                    .filter(
                        TaskInstance.dag_id == dag_id,
                        TaskInstance.run_id == run_id,
                        or_(
                            TaskInstance.pool == TARGET_POOL,
                            TaskInstance.task_id == 'freeze_watcher',
                        ),
                    )
                    .all()
                )

                for task in tasks:
                    if task.state != State.SUCCESS:
                        old_state = task.state
                        task.state = State.SUCCESS
                        logging.info(
                            "Задача %s.%s (pool=%s): %s -> SUCCESS",
                            dag_id, task.task_id, TARGET_POOL, old_state
                        )

                # Помечаем сам dag_run как успешный
                dag_run.state = State.SUCCESS
                logging.info("DAG run %s.%s помечен как SUCCESS", dag_id, run_id)

                unfrozen_dags.append(f"{dag_id}:{run_id}")
                success_count += 1

            except Exception as e:
                logging.error(
                    "Ошибка при разморозке %s.%s: %s",
                    dag_id, run_id, e, exc_info=True
                )
                fail_count += 1

        # Коммитим изменения
        session.commit()

    # Сохраняем результаты в XCom
    ti.xcom_push(key="unfrozen_count", value=success_count)
    ti.xcom_push(key="unfrozen_dags", value=unfrozen_dags)
    ti.xcom_push(key="fail_count", value=fail_count)

    logging.info(
        "Разморозка завершена: успешно=%d, ошибок=%d",
        success_count, fail_count
    )

    if fail_count > 0:
        raise AirflowException(
            f"Некоторые DAG'ы не удалось разморозить: {fail_count} ошибок"
        )


def _printResults(**context):
    """Вывести результаты разморозки в лог."""
    ti = context["ti"]

    count = ti.xcom_pull(key="unfrozen_count", task_ids="mark_success")
    dags = ti.xcom_pull(key="unfrozen_dags", task_ids="mark_success")
    fails = ti.xcom_pull(key="fail_count", task_ids="mark_success")

    logging.info("=" * 60)
    logging.info("РЕЗУЛЬТАТЫ РАЗМОРОЗКИ (PROD)")
    logging.info("=" * 60)
    logging.info("Успешно разморожено: %d", count or 0)
    logging.info("Ошибок: %d", fails or 0)

    if dags:
        logging.info("Размороженные DAG'и:")
        for dag in dags:
            logging.info("  - %s", dag)

    logging.info("=" * 60)


# ----------------------------------------------------------------------------
#                           DAG Definition
# ----------------------------------------------------------------------------

with DAG(
    dag_id="unfreeze_all_dags_prod",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,  # Только один запуск одновременно
    tags=["system", "maintenance", "watcher", "prod", "DbSync"],
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
    doc_md=__doc__,
) as dag:

    configureLogger()

    mark_success_task = PythonOperator(
        task_id="mark_success",
        provide_context=True,
        python_callable=_markDagRunsAsSuccess,
        pool=TARGET_POOL,
    )

    print_results_task = PythonOperator(
        task_id="print_results",
        provide_context=True,
        python_callable=_printResults,
        trigger_rule="all_done",
        pool=TARGET_POOL,
    )

    mark_success_task >> print_results_task
