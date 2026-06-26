from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils import timezone
from airflow.settings import Session
from airflow.models import XCom, TaskInstance, DagRun
from airflow.models.log import Log
from airflow.jobs.job import Job
import logging
import time
from sqlalchemy import tuple_, text

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
}


def get_table_size(session, table_name):
    """Получает размер таблицы в строках"""
    try:
        result = session.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()
        return result
    except Exception as e:
        logging.getLogger("airflow.task").warning(f"Не удалось получить размер таблицы {table_name}: {str(e)}")
        return 1000000  # значение по умолчанию для больших таблиц


def ensure_indexes(session, logger):
    """Проверяет и создает необходимые индексы для оптимальной работы очистки"""
    indexes = [
        ("idx_dag_run_start_date", "dag_run", "start_date"),
        ("idx_task_instance_start_date", "task_instance", "start_date"),
        ("idx_log_dttm", "log", "dttm"),
        ("idx_job_latest_heartbeat", "job", "latest_heartbeat"),
        ("idx_xcom_run_id", "xcom", "run_id")
    ]

    for idx_name, table, column in indexes:
        # Проверяем существование индекса
        exists = session.execute(
            text("""
            SELECT 1 FROM pg_indexes 
            WHERE indexname = :idx_name
            """),
            {"idx_name": idx_name}
        ).scalar()

        if not exists:
            logger.info(f"Создаем индекс {idx_name} на {table}({column})...")
            try:
                # Создаем отдельное соединение с autocommit для CONCURRENTLY
                from airflow.settings import engine
                with engine.connect() as conn:
                    # Отключаем неявные транзакции
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    # Выполняем CREATE INDEX CONCURRENTLY в отдельной транзакции
                    conn.execute(
                        text(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table} ({column})")
                    )
                    logger.info(f"Индекс {idx_name} успешно создан CONCURRENTLY")
            except Exception as e:
                logger.warning(f"Не удалось создать индекс {idx_name} CONCURRENTLY: {str(e)}")
                try:
                    # Попробуем создать обычный индекс (с блокировкой)
                    logger.info(f"Попытка создания обычного индекса {idx_name}...")
                    # Сначала закрываем текущую транзакцию
                    session.commit()
                    # Создаем обычный индекс
                    session.execute(
                        text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({column})")
                    )
                    session.commit()
                    logger.info(f"Обычный индекс {idx_name} успешно создан")
                except Exception as e2:
                    logger.warning(f"Не удалось создать обычный индекс {idx_name}: {str(e2)}")


def safe_delete(session, query, max_retries=3, logger=None):
    """Безопасное удаление с повторными попытками"""
    for attempt in range(max_retries):
        try:
            result = query.delete(synchronize_session=False)
            session.commit()
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait_time = 2 ** attempt
            if logger:
                logger.warning(
                    f"Ошибка при удалении (попытка {attempt + 1}/{max_retries}): {str(e)}. Повтор через {wait_time} сек.")
            time.sleep(wait_time)
            session.rollback()
    return 0


def clean_table(session, model, filter_column, batch_size, delay, cleanup_stats, table_name, logger, cutoff_date):
    """Универсальная функция для очистки таблицы"""
    logger.info(f"Начинаем очистку {table_name}...")

    while True:
        # Создаем CTE
        if table_name == 'task_instance':
            # Для TaskInstance используем составной ключ
            cte = session.query(
                model.dag_id,
                model.task_id,
                model.run_id
            ).filter(
                getattr(model, filter_column) < cutoff_date
            ).limit(batch_size).cte(f"clean_{table_name}_cte")

            # Проверяем наличие записей
            if not session.query(cte.c.dag_id).limit(1).first():
                break

            # Удаляем через составной ключ
            from sqlalchemy import tuple_
            criteria = tuple_(
                model.dag_id,
                model.task_id,
                model.run_id
            ).in_(
                session.query(
                    cte.c.dag_id,
                    cte.c.task_id,
                    cte.c.run_id
                )
            )

            result = session.query(model).filter(criteria).delete(synchronize_session=False)
        else:
            # Для остальных таблиц
            id_column = model.run_id if hasattr(model, 'run_id') else model.id
            cte = session.query(model).with_entities(id_column).filter(
                getattr(model, filter_column) < cutoff_date
            ).limit(batch_size).cte(f"clean_{table_name}_cte")

            # Проверяем наличие записей
            if not session.query(cte.c[0]).limit(1).first():
                break

            result = session.query(model).filter(
                id_column.in_(session.query(cte.c[0]))
            ).delete(synchronize_session=False)

        session.commit()

        if result == 0:
            break

        cleanup_stats[table_name] += result
        logger.info(f"{table_name.capitalize()}: удалено {result} записей, итого: {cleanup_stats[table_name]}")
        time.sleep(delay)

    return cleanup_stats


def batch_cleanup_airflow_db(**context):
    """
    Оптимизированная очистка для очень больших БД Airflow 2.7.2
    Учитывает новые связи между моделями (TaskInstance -> DagRun, XCom -> DagRun)
    """
    session = Session()
    cutoff_date = timezone.utcnow() - timedelta(days=90)
    logger = logging.getLogger("airflow.task")

    try:
        # Проверяем и создаем необходимые индексы
        ensure_indexes(session, logger)

        # Получаем размеры таблиц для динамической настройки
        table_sizes = {
            'task_instance': get_table_size(session, 'task_instance'),
            'dag_run': get_table_size(session, 'dag_run'),
            'log': get_table_size(session, 'log'),
            'job': get_table_size(session, 'job'),
            'xcom': get_table_size(session, 'xcom')
        }

        # Динамические размеры пакетов
        batch_sizes = {
            'task_instance': min(50000, max(10000, table_sizes['task_instance'] // 100)),
            'dag_run': min(50000, max(10000, table_sizes['dag_run'] // 100)),
            'log': min(60000, max(15000, table_sizes['log'] // 100)),
            'job': min(70000, max(20000, table_sizes['job'] // 100)),
            'xcom': min(80000, max(20000, table_sizes['xcom'] // 100))
        }

        # Динамические задержки
        delays = {
            'task_instance': min(10, max(5, 10 - (table_sizes['task_instance'] // 1000000))),
            'dag_run': min(10, max(5, 10 - (table_sizes['dag_run'] // 1000000))),
            'log': min(5, max(2, 5 - (table_sizes['log'] // 2000000))),
            'job': min(4, max(1, 4 - (table_sizes['job'] // 2000000))),
            'xcom': min(3, max(1, 3 - (table_sizes['xcom'] // 2000000)))
        }

        logger.info(f"Оптимальные размеры пакетов: {batch_sizes}")
        logger.info(f"Оптимальные задержки: {delays}")

        cleanup_stats = {
            'xcom': 0,
            'task_instance': 0,
            'dag_run': 0,
            'log': 0,
            'job': 0
        }

        """# 1. Очистка XCom
        cleanup_stats = clean_table(
            session, DagRun, 'start_date',
            batch_sizes['xcom'], delays['xcom'],
            cleanup_stats, 'xcom', logger, cutoff_date
        )"""

        # 1. Очистка XCom (специальная обработка)
        logger.info("Начинаем очистку XCom...")
        while True:
            # Получаем run_id из DagRun для старых записей
            subquery = session.query(DagRun.run_id).filter(
                DagRun.start_date < cutoff_date
            ).limit(batch_sizes['xcom']).subquery()

            # Проверяем, есть ли записи для удаления
            if not session.query(subquery.c.run_id).limit(1).first():
                print('empty')
                break

            # Удаляем XCom записи, связанные со старыми DagRun
            result = session.query(XCom).filter(
                XCom.run_id.in_(subquery)
            ).delete(synchronize_session=False)

            session.commit()

            # Если ничего не удалено, выходим из цикла
            if result == 0:
                break

            cleanup_stats['xcom'] += result
            logger.info(f"XCom: удалено {result} записей, итого: {cleanup_stats['xcom']}")
            time.sleep(delays['xcom'])

        # 2. Очистка TaskInstance
        cleanup_stats = clean_table(
            session, TaskInstance, 'start_date',
            batch_sizes['task_instance'], delays['task_instance'],
            cleanup_stats, 'task_instance', logger, cutoff_date
        )

        # 3. Очистка DagRun
        cleanup_stats = clean_table(
            session, DagRun, 'start_date',
            batch_sizes['dag_run'], delays['dag_run'],
            cleanup_stats, 'dag_run', logger, cutoff_date
        )

        # 4. Очистка Log
        cleanup_stats = clean_table(
            session, Log, 'dttm',
            batch_sizes['log'], delays['log'],
            cleanup_stats, 'log', logger, cutoff_date
        )

        # 5. Очистка Job
        cleanup_stats = clean_table(
            session, Job, 'latest_heartbeat',
            batch_sizes['job'], delays['job'],
            cleanup_stats, 'job', logger, cutoff_date
        )

        total = sum(cleanup_stats.values())
        logger.info(f"ОЧИСТКА ЗАВЕРШЕНА. ВСЕГО УДАЛЕНО: {total} записей")
        logger.info(f"Детали: {cleanup_stats}")

        return {
            "xcom_deleted": cleanup_stats['xcom'],
            "task_instances_deleted": cleanup_stats['task_instance'],
            "dag_runs_deleted": cleanup_stats['dag_run'],
            "logs_deleted": cleanup_stats['log'],
            "jobs_deleted": cleanup_stats['job'],
            "total_deleted": total
        }

    finally:
        session.close()


def vacuum_full_tables():
    """Улучшенный VACUUM FULL с анализом таблиц, работающий напрямую с sql_alchemy_conn"""
    logger = logging.getLogger("airflow.task")

    try:
        # Получаем строку подключения напрямую из конфигурации
        from airflow.configuration import conf
        sql_alchemy_conn = conf.get('database', 'sql_alchemy_conn')
        logger.info(f"Используем sql_alchemy_conn для подключения (без имени соединения)")

        # Создаем соединение напрямую через sqlalchemy
        from sqlalchemy import create_engine
        engine = create_engine(sql_alchemy_conn)

        tables = [
            'log',
            'job',
            'xcom'
        ]

        for table in tables:
            try:
                # Анализируем таблицу перед VACUUM
                logger.info(f"Анализируем таблицу {table} перед VACUUM...")

                # Получаем размер таблицы
                with engine.connect() as conn:
                    size_before = conn.execute(
                        text(f"SELECT pg_total_relation_size('{table}')")
                    ).scalar()

                    logger.info(f"Размер таблицы {table} до VACUUM: {size_before / (1024 * 1024):.2f} MB")

                    # Выполняем VACUUM FULL
                    logger.info(f"Начинаем VACUUM FULL для {table}...")
                    start_time = time.time()

                    # Устанавливаем autocommit для VACUUM
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    conn.execute(text(f"VACUUM FULL VERBOSE {table}"))

                    duration = time.time() - start_time

                    # Проверяем размер после VACUUM
                    size_after = conn.execute(
                        text(f"SELECT pg_total_relation_size('{table}')")
                    ).scalar()

                    saved_space = size_before - size_after
                    percent_saved = (saved_space / size_before * 100) if size_before > 0 else 0

                    logger.info(f"VACUUM FULL для {table} завершен за {duration:.2f} секунд")
                    logger.info(
                        f"Размер после: {size_after / (1024 * 1024):.2f} MB, освобождено: {saved_space / (1024 * 1024):.2f} MB ({percent_saved:.2f}%)")
            except Exception as e:
                logger.error(f"Ошибка при VACUUM FULL {table}: {str(e)}")
                # Не прерываем весь процесс из-за одной таблицы

    except Exception as e:
        logger.error(f"Критическая ошибка при настройке подключения: {str(e)}")
        raise


with DAG(
        dag_id='massive_db_cleanup2',
        schedule_interval='0 23 * * *',
        start_date=datetime(2023, 1, 1),
        catchup=False,
        default_args=default_args,
        max_active_runs=1,
        tags=['maintenance', 'DbSync', 'critical'],
        # Увеличенные таймауты для больших БД
        default_view='graph',
        orientation='TB',
        is_paused_upon_creation=False
) as dag:
    batch_cleanup = PythonOperator(
        task_id='batch_cleanup_airflow_db',
        python_callable=batch_cleanup_airflow_db,
        execution_timeout=timedelta(hours=10)  # Достаточно для 17ГБ таблиц
    )

    vacuum_full = PythonOperator(
        task_id='vacuum_full_tables',
        python_callable=vacuum_full_tables,
        execution_timeout=timedelta(hours=15),  # VACUUM FULL может занимать много времени
        executor_config={
        'heartbeat_timeout': 3600,  # 1 час вместо стандартных 300 секунд
        'health_check_threshold': 3600
        }
    )

    batch_cleanup >> vacuum_full