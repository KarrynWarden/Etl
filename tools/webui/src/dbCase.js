// Регистр имени таблицы по диалекту БД: Oracle — ВЕРХНИЙ, Postgres — нижний.
//
// Это зеркало dag_builder.to_db_case, и расходиться им нельзя: сервер этим же
// правилом получает имя линии (tableNameEtlJobs), а оно сравнивается с
// etl_jobs.tablename ДОСЛОВНО, с учётом регистра. Пишет туда триггер ведущей —
// значит регистр должен быть регистром ведущей БД. Ошибись здесь, и перенос
// упадёт с «Конфиг для ключа ... не найден», а аудит молча не найдёт ни одной
// группы.
//
// Регистр меняется по каждой части через точку — схема и имя отдельно, сами
// разделители не трогаются. Содержимое кавычек не трогается вовсе: в кавычках
// регистр значим, и `sch."MixedCase"` — это именно MixedCase, а не MIXEDCASE.
// Незакрытая кавычка тоже считается кавычкой: имя набирают вживую, и до того,
// как поставлена вторая кавычка, портить набранное нельзя.
export function toDbCase(name, db) {
  const text = (name ?? '').trim()
  if (!text) return ''
  const f = db === 'Orcl' ? (s) => s.toUpperCase() : (s) => s.toLowerCase()
  return text
    .split('.')
    .map((part) => (part.startsWith('"') ? part : f(part)))
    .join('.')
}

// Имя без схемы: 'KOKNAEV.PRBDIR' -> 'PRBDIR'. Зеркало dag_builder.bare.
export function bare(table) {
  const parts = String(table ?? '').trim().split('.')
  return parts[parts.length - 1]
}

// Схема из имени ('KOKNAEV.PRBDIR' -> 'KOKNAEV.'), либо пустая строка.
export function schemaOf(table) {
  const text = String(table ?? '').trim()
  const at = text.lastIndexOf('.')
  return at < 0 ? '' : text.slice(0, at + 1)
}

// Перенести имя таблицы с одной стороны на другую.
//
// В корпусе имя совпадает у 69 линий из 87 — ради этих 69 кнопка и нужна. Но
// «совпадает» здесь про имя, а не про строку целиком: схемы у сторон сплошь
// разные (KOKNAEV.MEDREE_PRDISP → medree_prdisp, iaddress → KOKNAEV.IADDRESS).
// Поэтому переносится ИМЯ БЕЗ СХЕМЫ, в регистре принимающей БД, а схема, если
// она в поле уже набрана, остаётся своя — её человек писал не просто так.
export function carryName(source, target, targetDb) {
  const name = bare(source)
  if (!name) return target ?? ''
  return schemaOf(target) + toDbCase(name, targetDb)
}

// Имя ли это вообще. В колонках справочника со своим SELECT в ячейке ведущей
// может стоять ВЫРАЖЕНИЕ, у которого имени нет: `TRUNC(dt)`, `''`,
// многострочный CASE. Приводить такое «к регистру диалекта» нельзя — из
// `TRUNC(dt)` вышло бы `TRUNC(DT)`, то есть правка чужого запроса под видом
// косметики. Зеркало sp_builder._BARE_NAME_RE.
export function isPlainName(name) {
  return /^(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$#]*)$/.test(String(name ?? '').trim())
}

// Совпадает ли имя с диалектом. Нужно, чтобы отличить «человек только что
// ввёл» от «так лежит в конфиге с давних пор»: у восемнадцати линий
// справочников ведомая записана не тем регистром, работают они прекрасно
// (для неэкранированных имён обе БД регистр игнорируют), и переписывать их
// молча — значит выдать за правку человека то, чего он не делал.
export function matchesDbCase(name, db) {
  const text = (name ?? '').trim()
  return !text || text === toDbCase(text, db)
}
