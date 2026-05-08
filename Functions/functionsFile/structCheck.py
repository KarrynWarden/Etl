"""Проверка соответствия структуры таблицы / запроса json-эталону.

Эталоном считается json (column_name, data_type, data_scale, is_primary_key).
Допускается, что в БД колонок больше, чем в json — это нормально, лишние
просто не участвуют в ETL. А вот наоборот недопустимо: если в json есть
колонка, которой нет в БД, проверка проваливается.

data_scale и длина в проверке намеренно игнорируются — отличия в scale между
NUMBER (Oracle) и numeric (Postgres) не должны блокировать перенос.
"""


def _trimTuple(tup):
    """Привести (name, data_type) к каноничному виду для сравнения."""
    name, dtype = tup[0], tup[1]
    mapping = {
        "timestamp without time zone": "timestamp",
        "character varying": "varchar",
    }
    return (name, mapping.get(dtype, dtype))


def StructCheckDataBase(jsonStruct, cursor, structureCheckSql, tableName):
    """Сверка json-эталона со схемой таблицы из information_schema/all_tab_columns."""
    bareName = tableName.split(".")[-1]
    cursor.execute(structureCheckSql, {"TABLENAME": bareName})
    cursorStruct = cursor.fetchall()

    jsonMap = {tuple(item[:3]): item for item in jsonStruct}
    cursorMap = {tuple(item[:3]): item for item in cursorStruct}

    setJsonShort = set(jsonMap.keys())
    setCursorShort = set(cursorMap.keys())

    diff = setJsonShort - setCursorShort
    if diff:
        full = [jsonMap[d] for d in diff]
        print(f"Столбцы json, которых нет в БД ({tableName}): {full}")

    return setJsonShort.issubset(setCursorShort)


def _arrTestFromCursor(cursor, dataTypes):
    arr = []
    for descr in cursor.description:
        dataType = dataTypes.get(str(descr[1]), "UNKNOWN") \
            if isinstance(dataTypes, dict) else dataTypes(descr[1])
        arr.append((descr[0], dataType))
    return arr


def StructCheckOracleQuery(jsonStruct, cursor, selectSql):
    """Проверка структуры произвольного Oracle-запроса (через cursor.description)."""
    cursor.execute(f"SELECT * FROM ({selectSql}) WHERE 1 = 0")
    dataTypes = {
        "<cx_Oracle.DbType DB_TYPE_DATE>": "DATE",
        "<cx_Oracle.DbType DB_TYPE_VARCHAR>": "VARCHAR2",
        "<cx_Oracle.DbType DB_TYPE_NUMBER>": "NUMBER",
        "<cx_Oracle.DbType DB_TYPE_TIMESTAMP>": "DATE",
    }
    arrTest = _arrTestFromCursor(cursor, dataTypes)
    setJson = {_trimTuple(t) for t in jsonStruct}
    setCursor = set(arrTest)
    diff = setJson - setCursor
    if diff:
        print("Столбцы json, которых нет в выборке (Oracle):", diff)
    return setJson.issubset(setCursor)


def StructCheckPostgresQuery(jsonStruct, cursor, selectSql):
    """Проверка структуры произвольного Postgres-запроса."""
    cursor.execute("SELECT oid, typname FROM pg_type")
    dataTypes = {oid: name for oid, name in cursor.fetchall()}
    cursor.execute(selectSql + " LIMIT 0")
    arrTest = [
        (descr[0], dataTypes.get(descr[1], "unknown"))
        for descr in cursor.description
    ]
    setJson = {_trimTuple(t) for t in jsonStruct}
    setCursor = set(arrTest)
    diff = setJson - setCursor
    if diff:
        print("Столбцы json, которых нет в выборке (Post):", diff)
    return setJson.issubset(setCursor)
