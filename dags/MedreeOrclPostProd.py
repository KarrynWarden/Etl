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

from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger, runEtl


MEDREE_TABLES = ["medree_cons", "medree_consdet"]


with DAG(
    dag_id="MedreeOrclPostProd",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["OrclPost", "medree", "prod", "DbSync", "section"],
    schedule_interval=dt.timedelta(minutes=5),
    catchup=False,
) as dag:
    configureLogger()

    previous = None
    for tableName in MEDREE_TABLES:
        task = buildOperator(
            f"do_etl_{tableName}",
            runEtl(
                tableNameMaster=tableName,
                dbMaster="Orcl",
                dbSlave="Post",
            ),
        )
        if previous is not None:
            previous >> task
        previous = task
