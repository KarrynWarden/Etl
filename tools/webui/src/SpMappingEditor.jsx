import { useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Checkbox,
  Input,
  Modal,
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
  onNameMaster,
  onFixCase,
  onSnap,
  snapping,
}) {
  const [filter, setFilter] = useState('all')
  const [allowReuse, setAllowReuse] = useState(false)
  const [nameError, setNameError] = useState(null)
  const [lastSource, setLastSource] = useState(null)
  // {index, name} — какой колонке сейчас дают псевдоним. Единственная правка
  // запроса, оставшаяся на этой странице, поэтому она через отдельное окно с
  // подтверждением, а не «набрал в ячейке и ушёл».
  const [naming, setNaming] = useState(null)

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
  // Что вообще МОЖНО взять с ведущей стороны: колонки таблицы или выходные
  // колонки запроса. Не то же самое, что выбранное: с тех пор как список
  // колонок надстраивается НАД запросом (sp_builder.build_sp_select_over),
  // выбрать можно не всё — лишние колонки в запросе никому не мешают.
  const masterAvail = useMemo(
    () => [...new Set([
      ...(spec.master_cols || []).map(nameOf),
      ...masterNames.filter(Boolean),
    ])],
    [spec.master_cols, masterNames],
  )
  const unusedMasters = masterAvail.filter(
    (n) => n && isPlainName(n) && !masterNames.includes(n),
  )

  // Ячейка ведущей ВЫБИРАЕТ колонку, а не называет её. Это следствие обёртки:
  // внешний список ссылается на колонку запроса по имени, поэтому «назвать её
  // иначе» здесь нечем — имя живёт в запросе, и меняют его там. Раньше ячейка
  // была полем ввода, и правка имени тихо переписывала чужой SELECT.
  const setPair = (i, m, s) =>
    onChange({ pairs: pairs.map((p, j) => (j === i ? [m, s] : p)) })

  const setMaster = (i, name) => {
    const from = pairs[i][0]
    if (!name || name === from) return
    if (masterNames.some((n, j) => j !== i && n === name)) {
      setNameError(`Колонка ${name} уже выбрана другой парой.`)
      return
    }
    setNameError(null)
    setPair(i, name, pairs[i][1])
  }

  // Приведение регистра сразу у всех имён — по колонке за раз это полсотни
  // нажатий. Только ВЕДОМАЯ сторона: её имена конструктор и пишет (они идут в
  // INSERT), а имена ведущей — это имена колонок запроса или таблицы, и менять
  // их отсюда значило бы править чужой текст.
  const offCase = slaveNames.filter((n) => n && !matchesDbCase(n, spec.db_slave))
  const fixCase = () => {
    setNameError(null)
    onFixCase()
  }

  // Убрать колонку из ЛИНИИ, а не из запроса. Запрос при этом не меняется
  // вовсе: колонка просто перестаёт попадать во внешний список, который
  // конструктор надстраивает над ним. До обёртки так было нельзя — состав и
  // порядок SELECT обязаны были совпадать с INSERT, и удаление лезло в чужой
  // текст.
  const removeRow = (i) => onChange({ pairs: pairs.filter((_p, j) => j !== i) })

  // Добавить пару: сразу с первой свободной колонкой ведущей — выдумывать
  // «колонка N» больше нечего, ведущая сторона теперь ВЫБИРАЕТСЯ из готового
  // списка, а имени, которого в запросе нет, конструктор всё равно не примет.
  const addPair = () =>
    onChange({ pairs: [...pairs, [unusedMasters[0] || '', null]] })

  const addMissing = () =>
    onChange({ pairs: [...pairs, ...unusedMasters.map((m) => [m, null])] })

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
              ? `Привести имена колонок ведомой к регистру ${spec.db_slave}. Ведущую сторону не трогает: там имена колонок ЗАПРОСА, менять их можно только в нём самом.`
              : 'Регистр имён ведомой уже соответствует диалекту'
          }
        >
          <Button
            icon={<FontSizeOutlined />}
            disabled={!offCase.length}
            danger={offCase.length > 0}
            onClick={fixCase}
          >
            Регистр ведомой{offCase.length ? ` (${offCase.length})` : ''}
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
          message="Так не получится"
          description={nameError}
          onClose={() => setNameError(null)}
        />
      )}
      {/* Главное, что изменилось с появлением обёртки, — и сказать об этом
          надо один раз и тихо, а не предупреждением на каждый экран. */}
      {hasSql && pairs.length > 0 && (
        <Typography.Paragraph type="secondary" style={{ fontSize: 12 }}>
          Переносятся только сопоставленные колонки: конструктор надстраивает
          над вашим запросом <Typography.Text code>SELECT …</Typography.Text> с
          ними — поэтому лишние колонки в запросе безвредны, а их порядок
          ничего не решает.
        </Typography.Paragraph>
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
            // ВЫБОР колонки, а не её имя. Имя колонки живёт в запросе (или в
            // таблице), и меняют его там; здесь решают только, какая из них
            // поедет в ведомую и в каком порядке.
            render: (_v, r) => {
              // Колонка без имени: выражение вроде `TRUNC(dt)` или `''`.
              // Сослаться на неё по имени нельзя — внешний список именно так и
              // ссылается, — поэтому предлагаем ровно то, что нужно: дать
              // псевдоним. Это единственная правка запроса, которая осталась на
              // этой странице, и она явная.
              const plain = isPlainName(r.m)
              return plain ? (
                <ComboBox
                  style={{ width: '100%' }}
                  size="small"
                  value={r.m || ''}
                  options={masterAvail.filter(
                    (n) => n === r.m || !masterNames.includes(n),
                  )}
                  placeholder="выбрать колонку запроса"
                  onChange={(v) => setMaster(r.i, v ? v.trim() : '')}
                />
              ) : (
                <Space size={4}>
                  <Tooltip title={flat(r.m)}>
                    <Typography.Text code ellipsis style={{ maxWidth: 260 }}>
                      {flat(r.m)}
                    </Typography.Text>
                  </Tooltip>
                  <Tooltip
                    title={
                      locked
                        ? 'У колонки нет имени, а выбирается она по имени. Снимите замок, чтобы задать псевдоним в запросе.'
                        : 'У колонки нет имени, а выбирается она по имени — задайте псевдоним, он появится в запросе'
                    }
                  >
                    <Button
                      size="small"
                      danger
                      icon={<QuestionCircleOutlined />}
                      disabled={locked}
                      onClick={() => setNaming({ index: r.i, name: '' })}
                    >
                      дать имя
                    </Button>
                  </Tooltip>
                </Space>
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
                description="Запрос не изменится: колонка просто перестанет переноситься."
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
        <Button
          size="small"
          icon={<PlusOutlined />}
          disabled={!unusedMasters.length}
          onClick={addPair}
        >
          строку сопоставления
        </Button>
        {unusedMasters.length > 0 && (
          <Button size="small" onClick={addMissing}>
            добавить все свободные ({unusedMasters.length})
          </Button>
        )}
        <Typography.Text type="secondary">
          сопоставлено {pairs.length - unmatched} из {pairs.length}
          {unusedMasters.length > 0 &&
            `; ${unusedMasters.length} колонок ведущей не переносятся`}
        </Typography.Text>
      </Space>

      {/* Дать имя колонке — единственное, что эта страница пишет в запрос.
          Явно, с подтверждением и с показом того, к чему псевдоним приписан. */}
      <Modal
        open={Boolean(naming)}
        title="Дать колонке имя"
        okText="Приписать псевдоним"
        cancelText="Отмена"
        okButtonProps={{ disabled: !isPlainName((naming?.name || '').trim()) }}
        onCancel={() => setNaming(null)}
        onOk={() => {
          onNameMaster(naming.index, naming.name.trim())
          setNaming(null)
        }}
      >
        <Typography.Paragraph type="secondary">
          Колонка выбирается по имени, а у этой его нет — в запросе стоит
          выражение:
        </Typography.Paragraph>
        <Typography.Paragraph>
          <Typography.Text code>{flat(pairs[naming?.index]?.[0])}</Typography.Text>
        </Typography.Paragraph>
        <Input
          autoFocus
          placeholder="имя колонки, например excltypeline"
          value={naming?.name || ''}
          onChange={(e) => setNaming({ ...naming, name: e.target.value })}
        />
        <Typography.Paragraph type="secondary" style={{ marginTop: 8 }}>
          Псевдоним допишется в запрос к этому выражению — остальной текст
          останется как есть.
        </Typography.Paragraph>
      </Modal>
    </>
  )
}
