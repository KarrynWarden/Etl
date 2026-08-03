"""DAG: единый аудит всех линий переноса.

Один DAG на весь аудит — без разбиения по направлениям. Линии берутся из
config.json (iterAuditLines): по одной задаче на каждый ключ
tableNameEtlJobs+dbMaster+dbSlave, во всех направлениях (Orcl<->Post,
Post->Post, Orcl->Orcl).

Каждая задача = универсальный Do_audit: берёт из etl_jobs группы своей
линии с isokaudit != 1 (и != 4 — зона ETL), сравнивает данные ведущей и
ведомой за период и проставляет isokaudit (1 / -4 / -2). Статус коммитится
по каждой группе отдельно — поэтому даже если airflow пришлёт SIGTERM на
середине (проблема долгого iperson на ~1000 групп), проверенные группы
сохранятся, а оставшиеся доберёт следующий запуск. Отдельные «костыльные»
запуски в 17/18ч больше не нужны.

freeze_watcher после всех линий — та же логика, что и у ETL-дагов:
если все линии заморожены, удерживает DAG-run; если есть упавшие —
сводный статус 🟥.
"""
import re

from airflow.models import DAG

from Functions._dagHelpers import (
    DEFAULT_ARGS, configureLogger, makeAuditOperator, addFreezeWatcher,
    addEtlLock, iterAuditLines,
)


def _taskId(key):
    """Безопасный task_id из ключа конфига (точки и пр. -> '_')."""
    return "audit_" + re.sub(r"\W", "_", key)


with DAG(
    dag_id="AuditAll",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=128,
    tags=["audit", "DbSync"],
    # Аудит — в конце дня, ежечасно с 18:00 до 23:00. max_active_runs=1 не
    # даёт прогонам наслаиваться; если airflow прибьёт долгий iperson SIGTERM'ом
    # на 5-м часу (задача -> skip), следующий часовой запуск спокойно добирает
    # оставшиеся группы (isokaudit != 1) — это и заменяет старый костыль с
    # двумя запусками в 17/18ч, обобщая его на несколько попыток за вечер.
    schedule_interval="0 18,23 * * *",
    catchup=False,
) as dag:
    configureLogger()

    tasks = [
        makeAuditOperator(
            _taskId(line["key"]),
            tableNameMaster=line["tableNameMaster"],
            dbMaster=line["dbMaster"],
            dbSlave=line["dbSlave"],
            tableNameEtlJobs=line["tableNameEtlJobs"],
            retryMode="rare",
        )
        for line in iterAuditLines()
    ]

    addEtlLock(tasks)
    addFreezeWatcher(tasks, retryMode="rare")