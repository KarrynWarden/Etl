import { useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Checkbox,
  Input,
  Popconfirm,
  Segmented,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import {
  DeleteOutlined,
  FontSizeOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  ReloadOutlined,
} from '@ant-design/icons'

import ComboBox from './ComboBox'
import SqlLock from './SqlLock'
import { isPlainName, matchesDbCase } from './dbCase'

// Сопоставление колонок справочника.
//
// Отличие от сложного ETL одно, но существенное: здесь у колонок НЕТ типов.
// Линия описана парой готовых SQL, и всё, что о колонках известно, — это их
// имена из SELECT и из INSERT. Поэтому таблица проще, а всё остальное —
// переименование, приведение регистра, фильтр, запрет занимать чужую пару —
// работает так же, как там: разные страницы одного конструктора не должны
// вести себя по-разному.
//
// Сопоставление ПОЗИЦИОННОЕ: i-я колонка SELECT ложится в i-ю колонку INSERT.
// Имена нужны человеку, а не рантайму, — но именно из них собираются оба
// запроса, поэтому правка имени здесь и есть правка SQL.
const nameOf = (c) => (typeof c === 'string' ? c : c?.column_name || c?.COLUMN_NAME || '')

// Имя колонки — это либо обычный идентификатор, либо имя в кавычках. Всё
// остальное в этой ячейке — ВЫРАЖЕНИЕ без псевдонима: `TRUNC(dt)`, `''`,
// многострочный CASE. Имени у такой колонки нет ни в запросе, ни в БД, и
// показывать его как имя нечестно — но и прятать нельзя: сопоставлять-то её
// надо. Показываем как есть и рядом говорим, что делать: вписать имя, и
// псевдоним появится в запросе сам.
// Многострочное выражение в однострочном поле — каша из отступов. Схлопываем
// пробелы ДЛЯ ПОКАЗА; правку сравниваем и с исходным, и со схлопнутым, иначе
// уход из поля, которого никто не трогал, считался бы переименованием.
const flat = (s) => String(s || '').replace(/\s+/g, ' ').trim()

export default function SpMappingEditor({
  spec,
  locked,
  onLockChange,
  onChange,
  onRenameMaster,
  onRemoveMaster,
  onFixCase,
  onSnap,
  snapping,
}) {
  const [filter, setFilter] = useState('all')
  const [allowReuse, setAllowReuse] = useState(false)
  const [nameError, setNameError] = useState(null)
  const [lastSource, setLastSource] = useState(null)

  const hasTable = Boolean((spec.master_table || '').trim())
  const hasSlave = Boolean((spec.slave_table || '').trim())
  const hasSql = Boolean((spec.select_sql_text || '').trim())

  const pairs = spec.pairs || []
  // Имена ведомой ЖИВУТ В ПАРАХ, а не в отдельном списке: INSERT собирается
  // ровно из них (sp_builder.build_sp_all берёт s_order из pairs). Список
  // slave_cols — только подсказки, восстановленные из текущего Add.sql, и
  // править его отдельно значило бы править то, что ни во что не превращается.
  // Поэтому здесь объединение: и разобранное из файла, и уже набранное руками.
  const slaveNames = useMemo(
    () => [...new Set([
      ...(spec.slave_cols || []).map(nameOf),
      ...pairs.map(([, s]) => s).filter(Boolean),
    ])],
    [spec.slave_cols, pairs],
  )
  const masterNames = useMemo(() => pairs.map(([m]) => m), [pairs])
  const usedSlaves = useMemo(
    () => new Set(pairs.map(([, s]) => s).filter(Boolean)),
    [pairs],
  )

  // Режим правки ОДИН. «Из таблицы» и «из запроса» — это два способа
  // ЗАПОЛНИТЬ колонки, а не два способа их править: имя колонки ведущей
  // меняется здесь всегда, и всегда меняет запрос. Разница только в том, ЧТО
  // именно в запросе меняется, и это забота одного правила на сервере
  // (sp_builder.rename_select_column): голое имя заменяется целиком,
  // у выражения меняется псевдоним, а не было псевдонима — добавляется.
  const setPair = (i, m, s) =>
    onChange({ pairs: pairs.map((p, j) => (j === i ? [m, s] : p)) })

  const renameMaster = (i, event) => {
    const name = event.target.value.trim()
    const from = pairs[i][0]
    if (name === from || name === flat(from)) return
    if (!name || masterNames.some((n, j) => j !== i && n === name)) {
      setNameError(
        !name
          ? `Пустое имя колонке не годится — ${from} оставлен как был.`
          : `Колонка ${name} в запросе уже есть — имена должны быть разными.`,
      )
      event.target.value = from
      return
    }
    setNameError(null)
    onRenameMaster(i, name)
  }

  // Приведение регистра сразу у всех имён — по колонке за раз это полсотни
  // нажатий. Ведущая — только при открытом замке и только там, где в ячейке
  // ИМЯ: у выражения регистр менять нечему, из `TRUNC(dt)` вышло бы `TRUNC(DT)`.
  const offCase = [
    ...(locked
      ? []
      : masterNames.filter(
          (n) => n && isPlainName(n) && !matchesDbCase(n, spec.db_master),
        )),
    ...slaveNames.filter((n) => n && !matchesDbCase(n, spec.db_slave)),
  ]
  const fixCase = () => {
    setNameError(null)
    onFixCase()
  }

  // Удаление идёт ОДНОЙ дорогой — через страницу, которая уберёт колонку и из
  // пар, и из SELECT. Отфильтровать пары здесь было бы короче и неверно:
  // рантайм справочника кладёт строку выборки в INSERT без проекции, так что
  // колонка, оставшаяся в запросе, роняет каждый прогон. Работает и при
  // запертом запросе — вырезается ровно она одна.
  const removeRow = (i) => onRemoveMaster(i)

  const addPair = () =>
    onChange({ pairs: [...pairs, [`колонка ${pairs.length + 1}`, null]] })

  const rows = pairs
    .map(([m, s], i) => ({ i, m, s }))
    .filter((r) => (filter === 'matched' ? Boolean(r.s) : filter === 'unmatched' ? !r.s : true))

  const unmatched = pairs.filter(([, s]) => !s).length
  const doubled = [...new Set(slaveNames.filter(
    (n) => pairs.filter(([, s]) => s === n).length > 1,
  ))]
  const orphanSlaves = slaveNames.filter((n) => !usedSlaves.has(n))

  const slaveOptions = (mine) =>
    allowReuse ? slaveNames : slaveNames.filter((n) => n === mine || !usedSlaves.has(n))

  return (
    <>
      {/* Откуда взять колонки — выбор на КАЖДОЕ снятие, а не свойство линии.
          «Из колонок» и «своё SELECT» — не два разных конструктора, между
          которыми надо выбрать раз и навсегда, а два инструмента, работающих
          вместе: написал запрос — снял по нему колонки; снял по таблице —
          получил из них запрос. Раньше кнопка молча смотрела на режим линии, и
          написанный руками SELECT она игнорировала, пока режим не переключат. */}
      <Space wrap style={{ marginBottom: 12 }}>
        <Space.Compact>
          <Tooltip
            title={
              hasTable
                ? `Прочитать колонки таблицы ${spec.master_table} — так видны и те, что появились в ней недавно`
                : 'Не заполнена ведущая таблица на вкладке «Настройки»'
            }
          >
            <Button
              icon={<ReloadOutlined />}
              loading={snapping && lastSource === 'table'}
              disabled={!hasTable || !hasSlave}
              onClick={() => {
                setLastSource('table')
                onSnap('table')
              }}
            >
              Снять по таблице
            </Button>
          </Tooltip>
          <Tooltip
            title={
              hasSql
                ? 'Выполнить ВАШ запрос и взять имена колонок из его псевдонимов — ровно так их увидит рантайм'
                : 'Запрос пуст — впишите его на вкладке SQL'
            }
          >
            <Button
              icon={<ReloadOutlined />}
              loading={snapping && lastSource === 'query'}
              disabled={!hasSql || !hasSlave}
              onClick={() => {
                setLastSource('query')
                onSnap('query')
              }}
            >
              Снять по запросу
            </Button>
          </Tooltip>
        </Space.Compact>
        <Tooltip
          title={
            offCase.length
              ? `Привести к регистру диалекта: ведущая ${spec.db_master}, ведомая ${spec.db_slave}`
              : 'Регистр всех имён уже соответствует диалекту'
          }
        >
          <Button
            icon={<FontSizeOutlined />}
            disabled={!offCase.length}
            danger={offCase.length > 0}
            onClick={fixCase}
          >
            Регистр имён{offCase.length ? ` (${offCase.length})` : ''}
          </Button>
        </Tooltip>
        {/* Тот же переключатель, что на вкладке SQL, и намеренно тот же
            компонент: замок ставят, глядя на запрос, а натыкаются на него
            здесь — переименованием колонки и регистром. Отправлять за ним на
            соседнюю вкладку значило бы делать работу из ничего. */}
        {locked !== undefined && (
          <SqlLock locked={locked} onChange={onLockChange} />
        )}
        <Segmented
          value={filter}
          onChange={setFilter}
          options={[
            { value: 'all', label: `все (${pairs.length})` },
            { value: 'matched', label: `с парой (${pairs.length - unmatched})` },
            { value: 'unmatched', label: `без пары (${unmatched})` },
          ]}
        />
        <Checkbox checked={allowReuse} onChange={(e) => setAllowReuse(e.target.checked)}>
          <Tooltip title="По умолчанию колонка ведомой предлагается только одной паре — так её не занять по ошибке.">
            разрешить одну колонку ведомой нескольким парам
          </Tooltip>
        </Checkbox>
      </Space>

      {nameError && (
        <Alert
          type="error"
          showIcon
          closable
          style={{ marginBottom: 12 }}
          message="Переименовать не вышло"
          description={nameError}
          onClose={() => setNameError(null)}
        />
      )}
      {!pairs.length && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="Колонок пока нет"
          description="Два пути, и они не исключают друг друга: заполнить обе таблицы на «Настройках» и нажать «Снять по таблице» — или вписать свой запрос на вкладке SQL и нажать «Снять по запросу»."
        />
      )}
      {doubled.length > 0 && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Одна колонка ведомой занята несколькими парами: ${doubled.join(', ')}`}
          description="В неё поедет последняя из них, остальные значения потеряются."
        />
      )}
      {orphanSlaves.length > 0 && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Есть в текущем Add.sql, но ни одной паре не отданы: ${orphanSlaves.join(', ')}`}
          description="INSERT собирается из пар, поэтому при следующей записи этих колонок в нём не будет — таблица их сохранит, но перенос заполнять перестанет."
        />
      )}

      <Table
        size="small"
        bordered
        pagination={false}
        scroll={{ y: 380 }}
        rowKey={(r) => r.i}
        dataSource={rows}
        columns={[
          { title: '#', dataIndex: 'i', width: 48, render: (v) => v + 1 },
          {
            title: `Ведущая (${spec.db_master})`,
            // Правится всегда. Что при этом произойдёт с запросом, зависит от
            // самого запроса, а не от «режима»: у `code` поменяется имя
            // колонки, у `m.code kod` — только псевдоним.
            render: (_v, r) => {
              const plain = isPlainName(r.m)
              const shown = plain ? r.m : flat(r.m)
              const hint = plain ? null : (
                <Tooltip
                  title={
                    <>
                      У этой колонки в запросе нет имени — это выражение:
                      <div style={{ fontFamily: 'monospace', margin: '4px 0' }}>
                        {flat(r.m)}
                      </div>
                      Впишите имя — и в запросе появится псевдоним, а колонка
                      станет называться так же, как её увидит рантайм.
                    </>
                  }
                >
                  <QuestionCircleOutlined style={{ color: '#faad14' }} />
                </Tooltip>
              )
              return locked ? (
                <Space size={4}>
                  <Typography.Text code ellipsis style={{ maxWidth: 320 }}>
                    {shown}
                  </Typography.Text>
                  {hint}
                </Space>
              ) : (
                <Input
                  size="small"
                  key={r.m}
                  defaultValue={shown}
                  suffix={hint}
                  style={{ fontFamily: 'monospace' }}
                  onBlur={(e) => renameMaster(r.i, e)}
                  onPressEnter={(e) => renameMaster(r.i, e)}
                />
              )
            },
          },
          {
            title: `Ведомая (${spec.db_slave})`,
            // Одно поле и выбирает, и называет: имя колонки ведомой живёт
            // здесь, и ровно оно попадает в INSERT. Список — подсказки; любое
            // другое имя вписывается как есть.
            render: (_v, r) => (
              <ComboBox
                style={{ width: '100%' }}
                size="small"
                value={r.s || ''}
                options={slaveOptions(r.s)}
                placeholder="выбрать из списка или вписать имя"
                onChange={(v) => setPair(r.i, r.m, v ? v.trim() : null)}
              />
            ),
          },
          {
            title: 'Имена',
            width: 110,
            render: (_v, r) =>
              r.s && r.m.toLowerCase() === String(r.s).toLowerCase() ? (
                <Typography.Text type="secondary">совпадают</Typography.Text>
              ) : (
                <Tag>{r.s ? 'различаются' : '—'}</Tag>
              ),
          },
          {
            title: '',
            width: 44,
            render: (_v, r) => (
              <Popconfirm
                title="Убрать колонку из линии?"
                description={
                  hasSql
                    ? `Уйдёт и из SELECT — вырежется ровно ${r.m}, остальной запрос останется как есть.`
                    : undefined
                }
                okText="Убрать"
                cancelText="Нет"
                onConfirm={() => removeRow(r.i)}
              >
                <Button size="small" type="text" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            ),
          },
        ]}
      />

      <Space style={{ marginTop: 12 }} wrap>
        {!locked && (
          <Button size="small" icon={<PlusOutlined />} onClick={addPair}>
            строку сопоставления
          </Button>
        )}
        <Typography.Text type="secondary">
          сопоставлено {pairs.length - unmatched} из {pairs.length}
        </Typography.Text>
        {locked && (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            убранная колонка уходит и из SELECT — рантайм кладёт строку выборки
            в INSERT как есть, лишняя колонка в запросе его роняет
          </Typography.Text>
        )}
      </Space>

    </>
  )
}
