"""Проверка соответствия структуры таблицы / запроса json-эталону.

Эталоном считается json (column_name, data_type, data_scale, is_primary_key).
Допускается, что в БД колонок больше, чем в json — это нормально, лишние
просто не участвуют в ETL. А вот наоборот недопустимо: если в json есть
колонка, которой нет в БД, проверка проваливается.

data_scale и длина в проверке намеренно игнорируются — отличия в scale между
NUMBER (Oracle) и numeric (Postgres) не должны блокировать перенос.

Тип, который драйвер не смог определить, тоже не блокирует перенос: у
константы в запросе (`null CHECKDIR`, `2 CHECKDIR`) тип из описания курсора
получается «UNKNOWN» / `unknown`, и сверять его не с чем — для таких колонок
проверяется только наличие имени в выборке.
"""

# Типы курсора cx_Oracle -> имена типов, как их пишут в json-структурах.
# Конструктор (tools/dag_builder.snap_query_structure) снимает типы по этому же
# словарю, поэтому снятая из запроса структура проходит проверку как есть.
ORACLE_CURSOR_TYPES = {
    "<cx_Oracle.DbType DB_TYPE_DATE>": "DATE",
    "<cx_Oracle.DbType DB_TYPE_VARCHAR>": "VARCHAR2",
    "<cx_Oracle.DbType DB_TYPE_NUMBER>": "NUMBER",
    "<cx_Oracle.DbType DB_TYPE_TIMESTAMP>": "DATE",
}

UNKNOWN_TYPES = ("UNKNOWN", "unknown")


def _isSubsetIgnoringUnknown(setJson, arrTest, source):
    """setJson ⊆ выборка, но колонки с неопределённым типом сверяются по имени.

    Возвращает True/False; расхождения печатает (их видно в логе задачи).
    """
    pairs = set(arrTest)
    names = {n for n, _t in arrTest}
    unknown = {n for n, t in arrTest if t in UNKNOWN_TYPES}
    missingCols, wrongType = [], []
    for item in setJson:
        name, dtype = item[0], item[1]
        if (name, dtype) in pairs or name in unknown:
            continue
        (wrongType if name in names else missingCols).append(item)
    if missingCols:
        print(f"Столбцов json нет в выборке ({source}):", missingCols)
    if wrongType:
        print(f"Тип столбцов json не совпал с выборкой ({source}):", wrongType,
              "— в выборке:", [(n, t) for n, t in arrTest
                               if n in {i[0] for i in wrongType}])
    return not missingCols and not wrongType


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
    print('here is structure ', tableName, tableName.split("."), ' and ', tableName.split(".")[-1],  structureCheckSql, bareName)
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
        print(setJsonShort, 'and', setCursorShort)

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
    arrTest = _arrTestFromCursor(cursor, ORACLE_CURSOR_TYPES)
    setJson = {_trimTuple(t) for t in jsonStruct}
    return _isSubsetIgnoringUnknown(setJson, arrTest, "Oracle")


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
    return _isSubsetIgnoringUnknown(setJson, arrTest, "Post")
