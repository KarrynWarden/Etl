"""DAG: oracle -> postgres для medree-таблиц.

Медри — отдельная семья таблиц с собственной логикой "срезов":
   - dcalc выступает в роли createdate (поле периода).
   - Сравниваются массивы уникальных (dcalc, MAX(lastupdate)) на ведущей
     и ведомой сторонах. Если для какого-то dcalc lastupdate отличается
     или его нет на ведомой — dcalc попадает в массив на обновление.
   - Дополнительно проверяется etl_log_iud_row: если для tablename есть
     записи с isetl=0, их period (=dcalc) тоже попадает в массив.
   - Для каждого dcalc из массива: полностью удаляем строки этого dcalc
     на ведомой и перезаливаем заново из ведущей.
   - Записи etl_log_iud_row, существовавшие на момент старта запуска,
     помечаются isetl=1 (новые останутся для следующего запуска).

Всё это инкапсулировано в режиме section_compare ядра do_etl.
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl, makeEtlOperator, addFreezeWatcher


MEDREE_TABLES = ["medree_cons", "medree_consdet", "koknaev.MEDREE_STRUCTURE_STACIONAR"]


with DAG(
    dag_id="MedreeOrclPostTest",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "medree", "test", "DbSync", "section"],
    schedule_interval=dt.timedelta(minutes=3),
    catchup=False,
) as dag:
    configureLogger()

    previous = None
    tasks = [
        makeEtlOperator(
            f"do_etl_{group}",
            tableNameMaster=group, dbMaster="Orcl", dbSlave="Post",
            tableNameEtlJobs=group, retryMode="frequent",
        )
        for group in MEDREE_TABLES
    ]
    #addPauseWatcher(tasks)
    addFreezeWatcher(tasks, retryMode="frequent")
    #for tableName in MEDREE_TABLES:
    #    task = buildOperator(
    #        f"do_etl_{tableName}",
    #        runEtl(
    #            tableNameMaster=tableName,
    #            dbMaster="Orcl",
    #            dbSlave="Post",
    #        ),
    #        triggerRule ="all_done"
    #    )
        #if previous is not None:
        #    previous >> task
        #previous = task
