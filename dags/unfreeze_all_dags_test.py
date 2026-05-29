"""DAG для ручного размораживания всех активных DAG'ов в TEST окружении после исправления
системной ошибки.

Версия 2.0: Размораживает ВСЕ запуски с fatal-ошибками (не только последний).

Логика:
- Находит ВСЕ запуски (running + failed) с error_class='fatal'
- Помечает их как успешные
- НЕ трогает запуски с error_class='record' (реальные проблемы с данными)
- НЕ трогает запуски с error_class='retryable' (временные проблемы)
"""
import datetime as dt
import logging
import json
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance, XCom, DagTag, DagModel
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import utcnow
from sqlalchemy import or_

try:
    from Functions._dagHelpers import configureLogger
except ImportError:
    def configureLogger():
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("airflow.task").setLevel(logging.INFO)

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 10, 23),
    "timezone": "Asia/Yekaterinburg",
}

TARGET_POOL = "Test"


def _getAllFatalDagRuns(**context):
    """Найти ВСЕ DAG runs с fatal-ошибками (не только активные).

    Возвращает список кортежей (dag_id, run_id) для разморозки.

    Критерии:
    1. DAG активен и имеет тег 'test'
    2. Запуск в состоянии 'running' или 'failed'
    3. ВСЕ задачи в пуле Test имеют error_class='fatal'
    4. Нет задач с error_class='record' или 'retryable'
    """
    with create_session() as session:
        logger = logging.getLogger("airflow.task")

        # 1. Получаем список DAG'ов с тегом 'test'
        active_dag_ids = (
            session.query(DagModel.dag_id)
            .filter(
                DagModel.is_active == True,
                DagModel.dag_id != 'unfreeze_all_dags_test'
            )
            .join(DagModel.tags)
            .filter(DagTag.name == 'test')
            .all()
        )
        active_dag_ids = [row[0] for row in active_dag_ids]

        if not active_dag_ids:
            logger.info("❌ Не найдено активных DAG'ов с тегом 'test'")
            return []

        logger.info(f"✅ Активные DAG'и с тегом test: {len(active_dag_ids)}")

        # 2. Находим ВСЕ запуски (running + failed) для этих DAG'ов
        all_runs = (
            session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
            .filter(
                DagRun.dag_id.in_(active_dag_ids),
                DagRun.state.in_([State.RUNNING, State.FAILED]),
            )
            .order_by(DagRun.dag_id, DagRun.execution_date.desc())
            .all()
        )

        logger.info(f"🔍 Найдено запусков (running + failed): {len(all_runs)}")

        if not all_runs:
            logger.info("⚠️ Нет запусков для проверки")
            return []

        candidates = []

        # 3. Перебираем каждый запуск
        for dag_id, run_id, run_state in all_runs:
            logger.info("-" * 80)
            logger.info(f"🔄 ПРОВЕРКА: {dag_id} | {run_id} | state={run_state}")

            # Получаем все задачи этого запуска
            tasks = (
                session.query(TaskInstance.task_id, TaskInstance.state, TaskInstance.pool)
                .filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == run_id,
                    TaskInstance.task_id != 'freeze_watcher',
                )
                .all()
            )

            if not tasks:
                logger.info(f"   ⛔ ПРОПУСК: Нет задач")
                continue

            # Фильтруем по целевому пулу
            pool_tasks = [t for t in tasks if t[2] == TARGET_POOL]
            if not pool_tasks:
                logger.info(f"   ⛔ ПРОПУСК: Нет задач в пуле '{TARGET_POOL}'")
                continue

            logger.info(f"   ℹ️ Задач в пуле {TARGET_POOL}: {len(pool_tasks)}")

            # Проверяем каждую задачу
            has_fatal = False
            has_record = False
            has_retryable = False
            has_other_errors = False

            for task_id, state, pool in pool_tasks:
                # Получаем error_class
                xcom_error = (
                    session.query(XCom.value)
                    .filter(
                        XCom.dag_id == dag_id,
                        XCom.task_id == task_id,
                        XCom.run_id == run_id,
                        XCom.key == 'error_class',
                    )
                    .first()
                )

                error_class = None
                if xcom_error and xcom_error.value:
                    val = xcom_error.value
                    if isinstance(val, bytes):
                        val = val.decode('utf-8')
                    try:
                        if isinstance(val, str):
                            error_class = json.loads(val)
                        else:
                            error_class = val
                    except:
                        error_class = str(val)

                # Классифицируем ошибку
                if error_class == 'fatal':
                    has_fatal = True
                    logger.info(f"   ├─ [{task_id}]: FATAL ✅")
                elif error_class == 'record':
                    has_record = True
                    logger.info(f"   ├─ [{task_id}]: RECORD ⚠️ (оставляем)")
                elif error_class == 'retryable':
                    has_retryable = True
                    logger.info(f"   ├─ [{task_id}]: RETRYABLE ⚠️ (оставляем)")
                elif state == State.FAILED:
                    has_other_errors = True
                    logger.info(f"   ├─ [{task_id}]: FAILED без error_class ⚠️")
                elif state == State.SUCCESS:
                    logger.info(f"   ├─ [{task_id}]: SUCCESS ✓")
                elif state == State.SKIPPED:
                    logger.info(f"   ├─ [{task_id}]: SKIPPED")
                else:
                    logger.info(f"   ├─ [{task_id}]: {state}")

            # Решение: размораживаем только если ЕСТЬ fatal И НЕТ record/retryable
            if has_fatal and not has_record and not has_retryable and not has_other_errors:
                logger.info(f"   🎉 ПОДХОДИТ: Все ошибки fatal, размораживаем")
                candidates.append((dag_id, run_id))
            else:
                reasons = []
                if not has_fatal:
                    reasons.append("нет fatal-ошибок")
                if has_record:
                    reasons.append("есть record-ошибки")
                if has_retryable:
                    reasons.append("есть retryable-ошибки")
                if has_other_errors:
                    reasons.append("есть другие ошибки")
                logger.info(f"   ⛔ ОТКЛОНЕН: {', '.join(reasons)}")

        logger.info("-" * 80)
        logger.info(f"ИТОГО КАНДИДАТОВ: {len(candidates)}")
        return candidates


def _markAllFatalRunsAsSuccess(**context):
    """Пометить ВСЕ найденные запуски с fatal-ошибками как успешные."""
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]

    candidates = _getAllFatalDagRuns(**context)

    if not candidates:
        logger.info("🛑 Нет запусков с fatal-ошибками для разморозки")
        ti.xcom_push(key="unfrozen_count", value=0)
        ti.xcom_push(key="unfrozen_dags", value=[])
        return

    logger.info(f"🚀 Размораживаем {len(candidates)} запусков...")

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
                    logger.warning(f"⚠️ DAG run {dag_id}.{run_id} не найден")
                    fail_count += 1
                    continue

                # Помечаем задачи в целевом пуле как успешные.
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
                        task.end_date = utcnow()
                        logger.info(f"   ✅ Задача {task.task_id}: {old_state} -> SUCCESS")

                # Помечаем dag_run как успешный
                old_run_state = dag_run.state
                dag_run.state = State.SUCCESS
                dag_run.end_date = utcnow()
                logger.info(f"   🏁 DAG run {dag_id}.{run_id}: {old_run_state} -> SUCCESS")

                unfrozen_dags.append(f"{dag_id}:{run_id}")
                success_count += 1

            except Exception as e:
                logger.error(f"❌ Ошибка при разморозке {dag_id}.{run_id}: {e}", exc_info=True)
                fail_count += 1

        session.commit()

    ti.xcom_push(key="unfrozen_count", value=success_count)
    ti.xcom_push(key="unfrozen_dags", value=unfrozen_dags)
    ti.xcom_push(key="fail_count", value=fail_count)

    logger.info("=" * 60)
    logger.info(f"РАЗМОРОЗКА ЗАВЕРШЕНА: Успешно={success_count}, Ошибок={fail_count}")
    logger.info("=" * 60)

    if fail_count > 0:
        raise AirflowException(f"Некоторые запуски не удалось разморозить: {fail_count} ошибок")


def _printResults(**context):
    """Вывести результаты разморозки."""
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]

    count = ti.xcom_pull(key="unfrozen_count", task_ids="mark_success")
    dags = ti.xcom_pull(key="unfrozen_dags", task_ids="mark_success")
    fails = ti.xcom_pull(key="fail_count", task_ids="mark_success")

    logger.info("=" * 60)
    logger.info("📊 РЕЗУЛЬТАТЫ РАЗМОРОЗКИ (TEST)")
    logger.info("=" * 60)
    logger.info(f"Успешно разморожено: {count or 0}")
    logger.info(f"Ошибок: {fails or 0}")

    if dags:
        logger.info("Размороженные запуски:")
        for dag in dags:
            logger.info(f"  - {dag}")
    else:
        logger.info("Ни один запуск не был разморожен.")

    logger.info("=" * 60)


# ----------------------------------------------------------------------------
#                           DAG Definition
# ----------------------------------------------------------------------------
with DAG(
    dag_id="unfreeze_all_dags_test",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["system", "maintenance", "watcher", "test"],
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__,
) as dag:
    configureLogger()

    mark_success_task = PythonOperator(
        task_id="mark_success",
        provide_context=True,
        python_callable=_markAllFatalRunsAsSuccess,
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
