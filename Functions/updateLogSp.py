from datetime import datetime
from Src.generalQueries import addLogOrclSql, addLogPostSql


# функция, обновляющая логи справочников
def UpdateLogSp(tableName, dbMaster, action, cursor, con, code, count=None):
    curr_dt = datetime.now()  # получение текущего времени
    match code:
        case 1:
            status = 'OK'
            errtext = f'Обработано записей {count}'
        case -1:
            errtext = 'ошибка в ходе выполнения EtlSp'
            status = 'ERR'
    cursor.execute(
        addLogPostSql if dbMaster == "Post" else addLogOrclSql,
        {'ID_JOB': None, 'TIMEOPER': curr_dt, 'STATUS': status, 'ERRCODE': code,
         'ERRTEXT': errtext, 'ACTION': action, 'TABLENAME': tableName},
    )
    con.commit()
