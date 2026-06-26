from Connect import DbConnectOrcl, DbConnectPost
from Src.spQueries import (deleteAllSpSql, spSelectPostSql, spSelectOrclSql, spEtlJobsPostSql, spEtlJobsOrclSql)
from Functions.updateLogSp import UpdateLogSp
from psycopg2.extras import execute_batch, execute_values

def SpEtl(tableNameMaster, tableNameSlave, addAllSpSql, action, selectSpSql, dbMaster, dbSlave, mode):
    countRows = 0
    try:
        con = DbConnectPost() if dbMaster == "Post" else DbConnectOrcl()
        cursor = con.cursor()
        
        # Убрали проверку через spSelect* - теперь она выполняется на уровне do_etl_sp()
        print(f'Начато обновление справочника {tableNameMaster}')
        
        # Подготовка slave-соединения
        con2 = DbConnectOrcl() if dbMaster == "Post" else DbConnectPost()
        cursor2 = con2.cursor()
        
        # Очистка целевой таблицы
        cursor2.execute(deleteAllSpSql.format(tableNameSlave))
        print(f'Справочник {tableNameSlave} очищен')
        
        # Выборка данных из master
        cursor.execute(selectSpSql)
        batchSize = 50000
        cursor.arraysize = batchSize
        
        # Пакетная вставка
        while True:
            rows = cursor.fetchmany(batchSize)
            if not rows:
                break
            countRows += len(rows)
            if dbSlave == "Orcl":
                cursor2.executemany(addAllSpSql, rows)
            else:
                execute_values(cursor2, addAllSpSql, rows)

        
        # Фиксация изменений
        con2.commit()
        print(f'{tableNameSlave} успешно обновлен ({countRows} записей)')
        
        # Логирование успеха
        UpdateLogSp(tableNameSlave, dbMaster, action, cursor, con, 1, countRows)
        return True
        
    except Exception as error:
        print(f'Ошибка при обновлении {tableNameSlave}: {str(error)}')
        if 'con2' in locals():
            con2.rollback()
        UpdateLogSp(tableNameSlave, dbMaster, action, cursor, con, -1, countRows)
        return False
        
    finally:
        # Закрытие соединений
        try:
            if 'cursor2' in locals(): cursor2.close()
            if 'con2' in locals(): con2.close()
        except:
            pass
        try:
            if 'cursor' in locals(): cursor.close()
            if 'con' in locals(): con.close()
        except:
            pass
        
