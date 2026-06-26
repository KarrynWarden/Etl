from Connect import DbConnectOrcl, DbConnectPost
from Src.spQueries import (deleteAllSpSql, spSelectPostSql, spSelectOrclSql, spEtlJobsPostSql, spEtlJobsOrclSql)
from Functions.updateLogSp import UpdateLogSp
from psycopg2.extras import execute_batch, execute_values


def SpEtl(tableNameMaster, tableNameSlave, addAllSpSql, action, selectSpSql, dbMaster, dbSlave, mode):
    count = 0
    try:
        con = DbConnectPost() if dbMaster == "Post" else DbConnectOrcl() #соединение с ведущей базой данных
        cursor = con.cursor() #создание курсора для ведущей таблицы
        if dbMaster == "Post":
            cursor.execute("select current_timestamp")
        else:
            cursor.execute("SELECT CURRENT_DATE FROM dual")
        curr_dt = cursor.fetchone()[0]
        #cursor.execute(spSelectPostSql if dbMaster == "Post" else spSelectOrclSql, {'tablename': tableNameMaster,})
        #savedLastUpdate = cursor.fetchone()
        if False:
            print(f'Справочник {tableNameMaster} не обновлялся')
        else:
            #получение всех записей таблицы
            print(tableNameMaster)
            print(selectSpSql)
            print(addAllSpSql)
            cursor.execute(selectSpSql)
            print('after select')
            batchSize = 100000
            #insertBatchSize = 10000
            cursor.arraysize = batchSize
            
            con2 = DbConnectOrcl() if dbMaster == "Post" else DbConnectPost() #соединение с ведомой базой данных
            cursor2 = con2.cursor() #создание курсора для ведомой таблицы
            #print(deleteAllSpSql.format(tableNameSlave))
            cursor2.execute(deleteAllSpSql.format(tableNameSlave))
            print('Справочник', tableNameSlave, 'был очищен')
            con2.commit()
            """try:
                if con2 is not None:
                    cursor2.close()
                    con2.close()
                    print(f"соединение {dbSlave} закрыто")
            except NameError:
                pass"""
            
            #print('before while')
            #countRows = 0
            rows = cursor.fetchmany()
            while rows:
                
                #countRows += len(rows)
                #print('fetched')
                """if not rows:
                    break"""
                #print(rows)
                print('start')
                #con2 = DbConnectOrcl() if dbMaster == "Post" else DbConnectPost() #соединение с ведомой базой данных
                #cursor2 = con2.cursor() #создание курсора для ведомой таблицы
                #for i in range(0, len(rows), insertBatchSize):
                #batch = rows[i:i + insertBatchSize]                    
                execute_values(cursor2, addAllSpSql, rows)
                con2.commit()
                print('end')
                rows = cursor.fetchmany()
                """try:
                    if con2 is not None:
                        cursor2.close()
                        con2.close()
                        print(f"соединение {dbSlave} закрыто")
                except NameError:
                    pass"""
                
                #execute_batch(
                #    cursor2,
                #    addAllSpSql,
                #    rows,
                #    page_size = batchSize
                #)
                #con2.commit()
                #print('inserted 50000')
            #with con.cursor(cursor_factory = DictCursor) as select_cursor:
                
                #select_cursor.itersize = 50000
                #select_cursor.execute(selectSpSql)
                #batch = []
                
                #print('after select')
                #arrRecords = cursor.fetchall()
            
                #batchSize = 50000
                #for record in select_cursor:
                #    batch.append(record)
                #    execute_batch(cursor2, addAllSpSql, batch)
                #    con.commit()
                #    batch = []
                #if batch:
                #    execute_batch(cursor2, addAllSpSql, batch)
                #    con.commit()
                #cursor2.executemany(addAllSpSql, arrRecords)
                    #batch = arrRecords[i:i+batchSize]
                    #execute_batch(cursor2, addAllSpSql, batch)
                    #con2.commit()
                    #del batch
                    #print('50000 ready') 

            #con2.commit()
            print(tableNameSlave, 'обновлен')
                  
            #UpdateLogSp(tableNameSlave, dbMaster, action, cursor, con, 1, countRows)
    except Exception as error:
        print('При выполнении произошла критическая ошибка..........', error)
        #con2.rollback()
        #UpdateLogSp(tableNameSlave, dbMaster, action, cursor, con, -1, countRows)
    finally:
        #если есть соединение, то закрываем его
        try:
            if con2 is not None:
                cursor2.close()
                con2.close()
                print(f"соединение {dbSlave} закрыто")
        except NameError:
            pass
        try:
            if con is not None:
                cursor.close()
                con.close()
                print(f"соединение {dbMaster} закрыто")
        except NameError:
            pass
        
