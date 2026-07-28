"""DAG: Orcl→Post для MEDREE_PRDISP — ЧАСТЬ 2 процесса (режим section).

Часть 1 (внутри Oracle, 02:00, koknaev.medree_prdisp_refresh) пересчитывает
Medree_prdisp за последние 18 месяцев и ставит ETL_JOBS.ISOKAUDIT = 4 тем
периодам (year, month), которые реально изменились, плюс тем, у которых
состояние в ETL_JOBS не в порядке (нет строки / -1 / -2 / -4) — так
расхождение, найденное аудитом, само уезжает на повторный перенос.

Гейт части 1 (есть ли medcheck.mekdt за вчера) срабатывает 1-2 раза в месяц,
поэтому подавляющее большинство запусков этого дага — пропуски (⬜).

Этот даг забирает из ETL_JOBS группы своей линии с ISOKAUDIT = 4 и переносит
каждую в Postgres полной перезаписью: DELETE группы в ведомой + заливка из
ведущей, затем LAST_SUCCESS_TS/ISOKAUDIT = 0 и запись в ETL_LOG. Групп с
ISOKAUDIT = 4 нет — задача завершается пропуском (⬜), ничего не трогая.

Расписание 05:00 и 07:00 — из постановки 827 (п. 8.3.1); оба запуска идут
после ночного джоба. Второй нужен как страховка: если в 05:00 часть групп не
успела или упала, они остаются с ISOKAUDIT = 4 и уезжают в 07:00.
retryMode='rare' — редкий запуск, ретраи делает airflow.
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="MedreeprdispOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "medree_prdisp", "DbSync", "section"],
    schedule_interval="0 5,7 * * *",
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "do_etl_medree_prdisp",
        tableNameMaster="KOKNAEV.MEDREE_PRDISP", dbMaster="Orcl", dbSlave="Post",
        tableNameEtlJobs="MEDREE_PRDISP",
        retryMode="rare",
    )
    addFreezeWatcher([task], retryMode="rare")
