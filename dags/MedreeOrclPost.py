"""DAG: составной перенос — несколько линий одним дагом.

Линии перечислены в LINES: по задаче на линию, все читают свои
настройки из etlFolder/config.d. Так собирают линии одного источника
(например, все doctype mocheck из общего MOCHECK.sql), чтобы не плодить
одинаковые даги и не гонять один и тот же запрос параллельно.

Файл пишет конструктор: правки в списке линий, расписании и тегах делаются
через него (иначе следующая дописанная линия их затрёт). Свободный текст
ниже маркера — исключение: его конструктор переносит в новый файл как есть.

─── заметка (правится руками, конструктор её сохраняет) ───
Медри — отдельная семья таблиц с собственной логикой «срезов» (режим
section_compare ядра do_etl, задан в config.d по линиям):

  * dcalc выступает в роли createdate (поле периода);
  * сравниваются срезы (dcalc, lastupdate, количество записей) на ведущей и
    ведомой. Группа обновляется, если разошлись множества lastupdate ЛИБО
    счётчики: одного lastupdate мало — удаление строки, не державшей максимум,
    множества не меняет, а данные уже другие;
  * дополнительно берутся группы с isokaudit = 4 в etl_jobs;
  * каждая такая группа перезаливается целиком: DELETE строк этого dcalc в
    ведомой + заливка из ведущей.

Журнал etl_log_iud_row эти линии НЕ читают (режим section_compare, а не
section_compare_with_iud) — триггер на ведущей им не нужен, и вкладка
«Триггеры» его с них не спрашивает.

MEDREE_PRDISP сюда не входит: у него своя механика (пересчёт джобом внутри
Oracle + режим section) и свой даг MedreeprdispOrclPost.
"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import (DEFAULT_ARGS, configureLogger, makeEtlOperator,
                                   addFreezeWatcher, lineEnabled)

# dagbuilder: составной даг (список линий ниже правит конструктор)
LINES = [
    ('MEDREE_CONS', 'Orcl', 'Post'),
    ('MEDREE_CONSDET', 'Orcl', 'Post'),
    ('koknaev.MEDREE_STRUCTURE_STACIONAR', 'Orcl', 'Post'),
]

with DAG(
    dag_id="MedreeOrclPost",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['OrclPost', 'medree', 'DbSync', 'section'],
    schedule_interval=dt.timedelta(minutes=3),
    catchup=False,
) as dag:
    configureLogger()
    tasks = [
        makeEtlOperator(
            f"do_etl_{line}",
            tableNameMaster=line, dbMaster=dbm, dbSlave=dbs,
            tableNameEtlJobs=line, retryMode="frequent",
        )
        for line, dbm, dbs in LINES
        if lineEnabled(line, dbm, dbs)
    ]
    if tasks:
        addFreezeWatcher(tasks, retryMode="frequent")
