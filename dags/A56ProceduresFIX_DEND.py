import datetime as dt
import logging
import colorlog
import sys
import json
import cx_Oracle
from collections import defaultdict

sys.path.append("../..")  # добавляем две родительские папки в sys.path
from Connect import DbConnectA56Post, DbConnectA56Orcl
from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 10, 23),
    'retries': 5,
    'depends_on_past': True,
    'retry_delay': dt.timedelta(minutes=1),
    # пояс не задаём намеренно — см. Functions/_dagHelpers.py,
    # кроны здесь написаны по UTC (5:50 = 10:50 в Екатеринбурге)
}

def do_etl_procedures():
    con = None
    con2 = None
    try:
        con = DbConnectA56Post()
        cursor = con.cursor()
        con2 = DbConnectA56Orcl()
        cursor2 = con2.cursor()
        
        # 7.1.1.1. Выбирать записи PG.IlogPRK c DWORK IS NULL
        # Добавляем TIMEOPER для сортировки и определения последней записи
        cursor.execute("""
            SELECT IDLOG, ID, DBEGIN, TYPE_OPER, SIGN, TIMEOPER
            FROM IlogPRK
            WHERE DWORK IS NULL and TYPE_OPER is not null
        """)
        arrRows = cursor.fetchall()
        
        if not arrRows:
            logging.info("Нет записей для обработки (DWORK IS NULL).")
            return

        # 7.1.1.1.1. Сгруппировать выбранные записи по ID
        groups = defaultdict(list)
        for row in arrRows:
            idlog, id_val, dbegin, type_oper, sign, timeoper = row
            groups[id_val].append({
                'idlog': idlog,
                'id_val': id_val,
                'dbegin': dbegin,
                'type_oper': type_oper,
                'sign': sign,
                'timeoper': timeoper
            })
            
        goodIds = []
        badIds = []
        rrIds = []
        
        for id_val, records in groups.items():
            # 7.1.1.1.1.1. Если в группе более одной записи с одинаковым TYPE_OPER, 
            # то изменить все, кроме последней по TIMEOPER: DWORK = тек время, дата, REVENT = RR
            type_oper_groups = defaultdict(list)
            for rec in records:
                type_oper_groups[rec['type_oper']].append(rec)
                
            valid_records_for_id = []
            
            for t_oper, t_records in type_oper_groups.items():
                # Сортируем по TIMEOPER по убыванию (последняя запись будет первой)
                # Используется dt.datetime.min для корректной сортировки, если TIMEOPER = None
                t_records.sort(
                    key=lambda x: (
                        x['timeoper'] if x['timeoper'] is not None else dt.datetime.min,
                        x['idlog']
                    ), 
                    reverse=True
                )
                
                # Самую последнюю запись оставляем для обработки
                valid_records_for_id.append(t_records[0])
                
                # Все предыдущие (устаревшие) помечаем как RR
                for old_rec in t_records[1:]:
                    rrIds.append(old_rec['idlog'])
                    
            # 7.1.1.1.3. Сортировать записи в группе по TYPE_OPER (1 -> 2)
            valid_records_for_id.sort(key=lambda x: x['type_oper'])
            
            # 7.1.1.1.2. Проверка ПЗСК в Postgres (последняя запись комплекта)
            pg_pzsk = None
            pg_pzsk_lastupdate = None
            try:
                cursor.execute("""
                    SELECT IDROW, date_trunc('second', lastupdate) as lastupdate FROM Iperson
                    WHERE ID = %s
                    ORDER BY DBEG DESC NULLS LAST
                    LIMIT 1
                """, (id_val,))
                pg_row = cursor.fetchone()
                if pg_row and str(pg_row[0]):
                    pg_pzsk = str(pg_row[0])
                    pg_pzsk_lastupdate = pg_row[1]
            except Exception as e:
                logging.warning(f"Ошибка проверки PG.Iperson для ID {id_val}: {e}")
                
            # Проверка ПЗСК в Oracle
            or_pzsk = None
            or_pzsk_lastupdate = None
            try:
                cursor2.execute("""
                    SELECT IDROW, lastupdate FROM (
                        SELECT IDROW, lastupdate FROM IPERSON
                        WHERE ID = :1
                        ORDER BY DBEG DESC NULLS LAST
                    ) WHERE ROWNUM = 1
                """, (id_val,))
                or_row = cursor2.fetchone()
                if or_row and str(or_row[0]).strip():
                    or_pzsk = str(or_row[0]).strip()
                    or_pzsk_lastupdate = or_row[1]
            except Exception as e:
                logging.warning(f"Ошибка проверки OR.IPERSON для ID {id_val}: {e}")
                
            # Условие синхронизации ПЗСК
            print(pg_pzsk, or_pzsk, ' and  ', pg_pzsk_lastupdate, or_pzsk_lastupdate)
            if pg_pzsk == or_pzsk and pg_pzsk_lastupdate == or_pzsk_lastupdate and pg_pzsk_lastupdate is not None and pg_pzsk is not None:
                # 7.1.1.1.3.1. Запустить последовательно процедуры
                for rec in valid_records_for_id:
                    try:
                        cursor2.execute("ALTER SESSION SET NLS_LANGUAGE = 'RUSSIAN'")
                        cursor2.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY HH24:MI:SS'")
                        cursor2.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ', '")
                        cursor2.setinputsizes(
                            cx_Oracle.NUMBER,   # :1 id_val
                            cx_Oracle.NUMBER,   # :2 1
                            cx_Oracle.DATETIME, # :3 dbegin
                            cx_Oracle.NUMBER,   # :4 type_oper
                            cx_Oracle.NUMBER    # :5 sign
                        )
                        cursor2.execute(
                            "BEGIN koknaev.a81add.UpdatePrkFromPG(:1, :2, :3, :4, :5); END;",
                            (rec['id_val'], 1, rec['dbegin'], rec['type_oper'], rec['sign'])
                        )
                        con2.commit()
                        goodIds.append(rec['idlog'])
                        logging.info(f"Успешно обработан IDLOG {rec['idlog']} (ID={rec['id_val']}, TYPE_OPER={rec['type_oper']})")
                    except Exception as error:
                        logging.error(f"Ошибка процедуры для IDLOG {rec['idlog']}: {error}")
                        con2.rollback()
                        badIds.append(rec['idlog'])
                        # Прерываем обработку группы. Если type_oper=1 упал, 
                        # то type_oper=2 запускать нельзя, он уйдет на следующий запуск.
                        break 
            else:
                # 7.1.1.1.4. Иначе группу не обрабатывать
                logging.info(f"Пропущена группа ID {id_val}: не соответствует условиям ПЗСК")
                continue
                
        # 7.1.1.1.3.2. Обновление статусов в ILOGPRK
        if goodIds:
            cursor.execute("""
                UPDATE ILOGPRK
                SET REVENT = 'OK', DWORK = CURRENT_TIMESTAMP
                WHERE IDLOG IN %s
            """, (tuple(goodIds),))
        if badIds:
            cursor.execute("""
                UPDATE ILOGPRK
                SET REVENT = 'ER', DWORK = CURRENT_TIMESTAMP
                WHERE IDLOG IN %s
            """, (tuple(badIds),))
        if rrIds:
            cursor.execute("""
                UPDATE ILOGPRK
                SET REVENT = 'RR', DWORK = CURRENT_TIMESTAMP
                WHERE IDLOG IN %s
            """, (tuple(rrIds),))
            
        con.commit()
        
    except Exception as error:
        logging.critical(f'Критическая ошибка в DAG: {error}')
        if con2:
            con2.rollback()
        if con:
            con.rollback()
    finally:
        # Безопасное закрытие соединений
        if con:
            cursor.close()
            con.close()
            print("Соединение Postgres закрыто")
        else:
            print("Соединение Postgres не было открыто")
            
        if con2:
            cursor2.close()
            con2.close()
            print("Соединение Oracle закрыто")
        else:
            print("Соединение Oracle не было открыто")

with DAG(dag_id='A56ProceduresFIX_DEND',
         default_args=args,
         max_active_runs=1,
         tags=["A56", "procedures", "DbSync"],
         schedule_interval='50 5,7,13 * * *',
         catchup=False) as dag:
         
    logger = logging.getLogger('airflow.task')
    handler = logging.StreamHandler()
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={
            'DEBUG': 'reset',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    do_etl_procedures_task = PythonOperator(
        task_id='do_etl_procedures',
        provide_context=True,
        python_callable=do_etl_procedures,
        dag=dag
    )
    
    do_etl_procedures_task