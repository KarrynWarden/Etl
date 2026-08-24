"""DAG-процедура: вызов pck_ferzl_load.ferzl_load() на Postgres A56

Обвязку ниже — расписание, теги, ретраи, соединения — пишет конструктор.
Тело do_etl_procedures() принадлежит вам: конструктор читает его в редактор и кладёт
обратно дословно, ни во что не вмешиваясь.

Задача идёт в пуле Etl (buildOperator), то есть НЕ стартует, пока идёт аудит:
задача-замок etl_lock занимает пул целиком. Для процедуры, которая правит
таблицы линий, это и нужно — иначе аудит поймает её на середине правки. Для
процедуры, к переносам не относящейся, это просто ожидание в очереди.
─── заметка (правится руками, конструктор её сохраняет) ───
depends_on_past прежней версии здесь нет намеренно: он вешал весь
DAG-run в нетерминальном состоянии, см. DEFAULT_ARGS.
"""
# dagbuilder: даг-процедура (обвязку правит конструктор, тело — ваше)
import datetime as dt
import logging

from airflow.models import DAG

from Connect import DbConnectA56Post
from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger

args = {**DEFAULT_ARGS,
        "retries": 5,
        "retry_delay": dt.timedelta(minutes=1)}


def do_etl_procedures(**context):
    con = None
    try:
        con = DbConnectA56Post()
        cursor = con.cursor()
        cursor.execute("call pck_ferzl_load.ferzl_load()")
        con.commit()
    except Exception as error:
        logging.error("При выполнении произошла критическая ошибка: %s", error)
    finally:
        if con is None:
            logging.info("соединение Postgres не было открыто")
        else:
            cursor.close()
            con.close()
            logging.info("соединение Postgres закрыто")


with DAG(
    dag_id="A56ProceduresFERZL_LOAD",
    default_args=args,
    max_active_runs=1,
    tags=['A56', 'procedures', 'DbSync'],
    schedule_interval=dt.timedelta(minutes=10),
    catchup=False,
) as dag:
    configureLogger()
    buildOperator("do_etl_procedures", do_etl_procedures)
