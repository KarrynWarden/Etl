import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Form,
  Input,
  Modal,
  Popconfirm,
  Radio,
  Row,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import { ReloadOutlined, UndoOutlined } from '@ant-design/icons'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import ComboBox from './ComboBox'
import FilesPreview from './FilesPreview'
import TableNamePair from './TableNamePair'
import { toDbCase } from './dbCase'
import SpMappingEditor from './SpMappingEditor'

// Справочники и разовый перенос.
//
// Отдельный от сложного ETL мир, и путать их дорого:
//   регулярный справочник переносится НЕ по расписанию, а по заданию — даг
//   SpEtlNew берёт из etl_jobs строки с isokaudit = 0 и гоняет все линии с
//   такой `dependence`. Ноль туда ставит АУДИТНЫЙ триггер на ведущей, и без
//   него справочник не обновится никогда, не выдав при этом ни одной ошибки;
//   разовый перенос запускают руками, сигнал ему не нужен вовсе.
//
// Линия описана не структурами, а парой готовых SQL (Select.sql / Add.sql),
// поэтому сопоставление колонок читается прямо из них — без обращения к БД.
const KINDS = { regular: 'Справочники', once: 'Разовый перенос' }
const DBS = [
  { value: 'Orcl', label: 'Oracle' },
  { value: 'Post', label: 'PostgreSQL' },
]

const WORK = { any: 'Все', on: 'В работе', off: 'Отключённые' }

// `kind` приходит СНАРУЖИ: справочники и разовый перенос — две вкладки
// верхнего уровня, а не переключатель внутри одной. Общего у них меньше, чем
// кажется по устройству файлов: справочник ждёт сигнала от аудитного триггера
// (isokaudit = 0) и обновляется сам, разовый запускают руками под конкретную
// задачу. Держать их на одной вкладке значило показывать вперемешку то, что и
// делают в разное время, и по разным поводам.
export default function SpPage({ kind }) {
  // Храним КЛЮЧ, а не сам объект линии. Объект после перезагрузки списка
  // становится копией из прошлого ответа, и форма продолжает показывать
  // прежнее состояние: выключаешь линию — в списке она краснеет, а
  // переключатель рядом стоит как стоял, потому что смотрит в устаревшую
  // копию. По ключу состояние всегда берётся из свежего списка.
  const [selectedKey, setSelectedKey] = useState(null)
  const [filter, setFilter] = useState('')
  const [work, setWork] = useState('any')
  const [direction, setDirection] = useState(null)
  // Создание новой линии — то же самое окно правки, но с пустой
  // спецификацией и без чтения с диска. Отдельной страницы не завожу: поля
  // ровно те же, а «создать» от «поправить» отличается только тем, откуда
  // взялись начальные значения и можно ли перезаписывать существующие файлы.
  const [creating, setCreating] = useState(false)

  const lines = useAction(api.spLines)

  useEffect(() => {
    lines.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const shown = useMemo(() => {
    const all = lines.result?.lines || []
    const needle = filter.trim().toLowerCase()
    return all.filter((l) => {
      if (l.kind !== kind) return false
      if (direction && l.direction !== direction) return false
      if (work === 'on' && l.disabled) return false
      if (work === 'off' && !l.disabled) return false
      return !needle || l.key.toLowerCase().includes(needle)
    })
  }, [lines.result, kind, filter, work, direction])

  // направления считаем по СВОЕМУ типу линий: на вкладке справочников
  // незачем предлагать направление, которое есть только у разового переноса
  const directions = useMemo(
    () => [...new Set((lines.result?.lines || [])
      .filter((l) => l.kind === kind).map((l) => l.direction))].sort(),
    [lines.result, kind],
  )

  const selected = useMemo(
    () => (lines.result?.lines || []).find((l) => l.key === selectedKey && l.kind === kind) || null,
    [lines.result, selectedKey, kind],
  )

  return (
    <Row gutter={16}>
      <Col xs={24} md={8} lg={6}>
        <Card
          size="small"
          title={KINDS[kind]}
          extra={
            <Space>
              <Button
                size="small"
                type="primary"
                onClick={() => {
                  setSelectedKey(null)
                  setCreating(true)
                }}
              >
                + Создать
              </Button>
              {/* значком, как в списке линий сложного ETL: одна и та же
                  кнопка не должна выглядеть на соседних вкладках по-разному */}
              <Tooltip title="Перечитать список">
                <Button
                  size="small"
                  icon={<ReloadOutlined />}
                  loading={lines.loading}
                  onClick={() => lines.run()}
                />
              </Tooltip>
            </Space>
          }
        >
          <Input.Search
            allowClear
            style={{ margin: '8px 0' }}
            placeholder="фильтр по имени"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          <Select
            style={{ width: '100%', marginBottom: 8 }}
            allowClear
            placeholder="направление"
            value={direction}
            onChange={setDirection}
            options={directions.map((d) => ({ value: d }))}
          />
          <Select
            style={{ width: '100%', marginBottom: 8 }}
            value={work}
            onChange={setWork}
            options={Object.entries(WORK).map(([value, label]) => ({ value, label }))}
          />
          <Typography.Text type="secondary">
            показано {shown.length} из {(lines.result?.lines || []).filter((l) => l.kind === kind).length}
          </Typography.Text>

          <ActionError error={lines.error} />

          <div style={{ maxHeight: '55vh', overflow: 'auto', marginTop: 8 }}>
            {(lines.loading && !lines.result ? [] : shown).map((l) => (
              <div
                key={l.key}
                onClick={() => {
                  setSelectedKey(l.key)
                  setCreating(false)
                }}
                style={{
                  cursor: 'pointer',
                  padding: '6px 8px',
                  borderBottom: '1px solid #f0f0f0',
                  background: l.key === selected?.key ? '#e6f4ff' : undefined,
                }}
              >
                <b>{l.key}</b>
                <div>
                  <Space size={4}>
                    <Tag>{l.direction}</Tag>
                    {l.disabled && <Tag color="red">отключена</Tag>}
                  </Space>
                </div>
              </div>
            ))}
            {lines.loading && !lines.result && <Spin />}
          </div>
        </Card>
      </Col>

      <Col xs={24} md={16} lg={18}>
        {/* key по выбранной линии — по той же причине, что и у сложного ETL:
            иначе состояние внутри формы (списки удаления, разборы, наборы в
            полях) переживает переход на другую линию и начинает относиться не к
            той. Создание — тоже отдельная «линия», отсюда new-<тип>. */}
        <SpForm
          key={creating ? `new-${kind}` : `${kind}:${selected?.key || 'none'}`}
          entry={creating ? { kind, key: null } : selected}
          existingKeys={(lines.result?.lines || []).map((l) => l.key)}
          onChanged={() => lines.run()}
          onCreated={(key) => {
            setCreating(false)
            setSelectedKey(key)
            lines.run()
          }}
        />
      </Col>
    </Row>
  )
}

// Начальная спецификация новой линии. Поля ровно те же, что отдаёт
// load_sp_line, — форма не должна знать, читали её с диска или завели сейчас.
function blankSpec(kind) {
  return {
    key: null,
    kind,
    master_label: '',
    db_master: 'Orcl',
    db_slave: 'Post',
    master_table: '',
    slave_table: '',
    dependence: '',
    disabled: false,
    append: false,
    doc: '',
    src_mode: 'table',
    select_sql: null,
    select_sql_text: '',
    add_sql: null,
    add_sql_text: '',
    master_cols: [],
    slave_cols: [],
    pairs: [],
    sql_dir_name: null,
  }
}

function SpForm({ entry, existingKeys, onChanged, onCreated }) {
  const [spec, setSpec] = useState(null)
  const [original, setOriginal] = useState(null)
  const [targets, setTargets] = useState(null)
  const [ddl, setDdl] = useState(null)

  const load = useAction(api.spLine)
  const preview = useAction(api.spPreview)
  const write = useAction(api.write)
  const move = useAction(api.spMove)
  const delTargets = useAction(api.spDeleteTargets)
  const remove = useAction(api.spDelete)
  const auditTrigger = useAction(api.spAuditTrigger)
  const snapMaster = useAction(api.snapStructure)
  const snapQuery = useAction(api.snapQueryStructure)
  const snapSlave = useAction(api.snapStructure)
  const match = useAction(api.match)
  const buildSql = useAction(api.spBuildSql)
  const parseSql = useAction(api.spParseSql)
  const renameCol = useAction(api.spRenameSelectColumn)

  const isNew = Boolean(entry) && !entry.key

  useEffect(() => {
    if (!entry) return
    preview.reset()
    write.reset()
    setTargets(null)
    if (!entry.key) {
      // Новая линия: читать нечего, начинаем с пустой формы. `original`
      // ставим тем же значением, чтобы «есть несохранённые правки» зажигалось
      // от первого настоящего ввода, а не от самого факта создания.
      const fresh = blankSpec(entry.kind)
      setSpec(fresh)
      setOriginal(JSON.stringify(fresh))
      return
    }
    load.run({ kind: entry.kind, key: entry.key }).then((d) => {
      if (!d) return
      setSpec(d.spec)
      setOriginal(JSON.stringify(d.spec))
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entry?.key, entry?.kind])

  const dirty = spec && original && JSON.stringify(spec) !== original

  if (!entry)
    return (
      <Card>
        <Typography.Text type="secondary">
          Выберите линию слева. Справочник обновляется по заданию (isokaudit = 0 в
          etl_jobs), разовый перенос — руками.
        </Typography.Text>
      </Card>
    )

  if (!isNew && load.loading && !spec)
    return (
      <Card>
        <Spin /> <Typography.Text type="secondary">Читаю линию…</Typography.Text>
      </Card>
    )

  if (load.error)
    return (
      <Card title={entry.key}>
        <ActionError error={load.error} />
      </Card>
    )

  if (!spec) return null

  const patch = (c) => setSpec((p) => ({ ...p, ...c }))
  const colName = (c) =>
    typeof c === 'string' ? c : c?.column_name || c?.COLUMN_NAME || ''
  const slaveNames = (spec.slave_cols || []).map(colName)

  // Снять колонки обеих сторон из БД.
  //
  // Нужно в двух случаях: у НОВОЙ линии колонок ещё нет вовсе, и у линии,
  // которую переводят с «всей таблицы» на явный список. Ведущая снимается по
  // тексту запроса, если режим «своё SELECT», — имена колонок там это его
  // псевдонимы, ровно их и увидит рантайм.
  //
  // Пары подбираются по именам (api.match) и только у тех колонок, для
  // которых пары ещё нет: уже расставленное руками не трогаем — это тот же
  // принцип, что на вкладке колонок сложного ETL.
  // Ключ фрагмента складывается на сервере (sp_key), здесь он нужен только
  // для заголовка новой линии — показать заранее, как она будет называться.
  const previewKey = `${spec.master_label || ''}${spec.db_master}${spec.db_slave}`

  const snapping =
    snapMaster.loading || snapQuery.loading || snapSlave.loading || match.loading
  const snapError =
    snapMaster.error || snapQuery.error || snapSlave.error || match.error

  // ── связь «колонки ↔ SQL» в обе стороны ─────────────────────────────────
  // Линия справочника ОПИСАНА двумя запросами: Select.sql и Add.sql. Значит
  // правка колонок — это правка запросов, и наоборот; держать их врозь и
  // показывать одно из двух только для чтения — значит заставлять человека
  // выбирать, каким способом он сегодня работает.
  //
  // Собирает и разбирает СЕРВЕР, теми же функциями, что пишут файлы
  // (build_sp_select / build_sp_add / restore_mapping). Повтори их здесь — и
  // текст в форме разошёлся бы с текстом в файле на первом же пробеле, а
  // предпросмотр показывал бы изменение сразу после того, как ничего не
  // меняли.
  const rebuildSql = async (next) => {
    const merged = { ...spec, ...next }
    const built = await buildSql.run({
      db_slave: merged.db_slave,
      master_table: merged.master_table,
      slave_table: merged.slave_table,
      src_mode: merged.src_mode,
      pairs: merged.pairs || [],
      // текущие тексты — чтобы сервер не переписывал раскладку рукописного
      // запроса там, где состав колонок не изменился
      select_sql_text: merged.select_sql_text || '',
      add_sql_text: merged.add_sql_text || '',
    })
    setSpec((p) => ({ ...p, ...next, ...(built || {}) }))
  }

  // Правка колонок: сразу пересобираем запросы.
  const patchMapping = (next) => {
    setSpec((p) => ({ ...p, ...next }))
    rebuildSql(next)
  }

  // Переименование колонки ведущей. РЕЖИМ ПРАВКИ ОДИН: имя меняется здесь
  // всегда и всегда меняет запрос. Что именно поменяется в тексте, решает не
  // «режим линии», а сам запрос — правило одно на сервере
  // (sp_builder.rename_select_column): голое имя заменяется целиком, у
  // выражения меняется псевдоним, не было псевдонима — добавляется.
  //
  // patchMapping в конце не случаен: в режиме «выбранные колонки» он соберёт
  // SELECT из имён заново и придёт к тому же результату, а в режиме «своё
  // SELECT» оставит наш переименованный текст. Одна дорога на оба случая.
  const renameMasterColumn = async (index, name) => {
    const pairs = (spec.pairs || []).map((p, i) =>
      i === index ? [name, p[1]] : p,
    )
    const sql = (spec.select_sql_text || '').trim()
    let select_sql_text = spec.select_sql_text
    if (sql) {
      const r = await renameCol.run({
        select_sql_text: spec.select_sql_text,
        index,
        new_name: name,
      })
      if (!r) return
      select_sql_text = r.select_sql_text
    }
    patchMapping({ pairs, select_sql_text })
  }

  // Регистр имён — тем же путём: каждое имя ведущей проходит через
  // переименование, поэтому запрос остаётся согласован со списком колонок.
  const fixColumnCase = async () => {
    let next = { ...spec }
    const pairs = (spec.pairs || []).map((p) => [...p])
    for (let i = 0; i < pairs.length; i++) {
      const want = toDbCase(pairs[i][0], spec.db_master)
      if (pairs[i][0] && want !== pairs[i][0]) {
        pairs[i][0] = want
        if ((next.select_sql_text || '').trim()) {
          const r = await renameCol.run({
            select_sql_text: next.select_sql_text,
            index: i,
            new_name: want,
          })
          if (r) next = { ...next, select_sql_text: r.select_sql_text }
        }
      }
      if (pairs[i][1]) pairs[i][1] = toDbCase(pairs[i][1], spec.db_slave)
    }
    patchMapping({ pairs, select_sql_text: next.select_sql_text })
  }

  // Правка запроса: разбираем его обратно в колонки. По уходу из поля, а не на
  // каждой букве — недописанный SELECT разбирать нечего, да и незачем.
  const reparseSql = async (next) => {
    const merged = { ...spec, ...next }
    const parsed = await parseSql.run({
      select_sql_text: merged.select_sql_text || '',
      add_sql_text: merged.add_sql_text || '',
    })
    setSpec((p) => ({ ...p, ...next, ...(parsed || {}) }))
  }

  // Снять колонки. `source` — откуда брать ВЕДУЩУЮ: 'table' или 'query'.
  //
  // Выбирает человек, на каждое снятие. «Из колонок» и «своё SELECT» — не два
  // конструктора, между которыми выбирают раз и навсегда, а инструменты,
  // работающие вместе: написал запрос — снял по нему колонки и разложил пары;
  // снял по таблице — получил из колонок готовый запрос. Раньше кнопка молча
  // смотрела на режим линии и написанный руками SELECT игнорировала, пока
  // режим не переключат отдельно, — то есть заставляла человека объявить, в
  // каком он «варианте», прежде чем сделать очевидное.
  //
  // Ведомая снимается всегда из своей таблицы: там двусмысленности нет.
  const snapColumns = async (source) => {
    const sql = (spec.select_sql_text || '').trim()
    const byQuery = source === 'query' && sql
    const m = byQuery
      ? await snapQuery.run({ db: spec.db_master, sql, table: spec.master_table })
      : await snapMaster.run({ db: spec.db_master, table: spec.master_table })
    if (!m) return
    const s = await snapSlave.run({ db: spec.db_slave, table: spec.slave_table })
    if (!s) return
    const known = new Map((spec.pairs || []).map(([mm, ss]) => [mm, ss]))
    const positional = (spec.pairs || []).map(([, ss]) => ss)
    const suggestion = await match.run({
      master_cols: m.columns,
      slave_cols: s.columns,
    })
    const next = {
      // Снятие по запросу означает, что хозяин запроса — человек: имена
      // колонок теперь берутся из его псевдонимов, и пересобирать SELECT из
      // колонок мы больше не вправе. Снятие по таблице — наоборот.
      src_mode: byQuery ? 'custom' : 'table',
      master_cols: m.columns,
      slave_cols: s.columns,
      pairs: m.columns.map((c, i) => {
        const name = colName(c)
        // 1) пара, расставленная руками; 2) для прежнего режима «вся таблица» —
        // порядок, на который он и полагался; 3) автоподбор по именам
        const kept = known.get(name)
        const byPosition = spec.src_mode === 'all' ? positional[i] : null
        return [name, kept || byPosition || suggestion?.suggestions?.[i] || null]
      }),
    }
    // По таблице — сразу пересобираем Select.sql: иначе колонки уже новые, а
    // запрос ещё старый, и увидеть это можно только в предпросмотре.
    // По запросу — не трогаем: он и есть источник.
    if (byQuery) setSpec((p) => ({ ...p, ...next }))
    else patchMapping(next)
  }

  // Занятый ключ — отказ при записи (overwrite: false), но узнать об этом
  // лучше сразу, а не после того, как заполнил всю форму.
  const keyTaken = isNew && spec.master_label && (existingKeys || []).includes(previewKey)

  const settings = (
    <Form layout="vertical">
      {isNew && (
        <Alert
          type={keyTaken ? 'error' : 'info'}
          showIcon
          style={{ marginBottom: 16 }}
          message={
            keyTaken
              ? `Линия ${previewKey} уже есть — выберите другую метку`
              : `Новая линия: ${KINDS[entry.kind].toLowerCase()}`
          }
          description={
            keyTaken
              ? 'Ключ фрагмента складывается из метки и пары БД, и он должен быть уникальным: запись откажется затирать чужие файлы.'
              : 'Заполните таблицы и метку, снимите колонки на вкладке «Колонки», затем «Предпросмотр» → «Записать».' +
                (entry.kind === 'regular'
                  ? ' После создания не забудьте про аудитный триггер: без него справочник не обновится ни разу и ошибки не выдаст.'
                  : '')
          }
        />
      )}
      <Row gutter={16}>
        <Col span={8}>
          <Form.Item label="БД ведущей">
            <Select value={spec.db_master} onChange={(v) => patch({ db_master: v })} options={DBS} />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="БД ведомой">
            <Select value={spec.db_slave} onChange={(v) => patch({ db_slave: v })} options={DBS} />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Метка линии" help="из неё складывается ключ фрагмента">
            <Input
              value={spec.master_label || ''}
              onChange={(e) => patch({ master_label: e.target.value })}
            />
          </Form.Item>
        </Col>
      </Row>

      <TableNamePair
        resetKey={entry.key || `new-${entry.kind}`}
        masterValue={spec.master_table || ''}
        masterDb={spec.db_master}
        onMasterChange={(v) => patch({ master_table: v })}
        slaveValue={spec.slave_table || ''}
        slaveDb={spec.db_slave}
        onSlaveChange={(v) => patch({ slave_table: v })}
      />

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label="Зависимость (dependence)"
            help="ровно эту строку даг сравнивает с etl_jobs.tablename — регистр значим"
          >
            <Input
              value={spec.dependence || ''}
              placeholder={spec.master_label}
              onChange={(e) => patch({ dependence: e.target.value })}
            />
          </Form.Item>
        </Col>
        <Col span={12}>
          {/* Переключателя «вариант конструктора» здесь больше нет. Колонки и
              свой SELECT — не два разных способа завести линию, между которыми
              надо выбрать заранее, а инструменты, работающие вместе: снял
              колонки по таблице — получил из них запрос; написал запрос — снял
              по нему колонки. Кто сейчас хозяин Select.sql, это СЛЕДСТВИЕ
              последнего действия, а не настройка, которую заполняют перед
              работой. Показываем состояние; меняется оно там, где и работают, —
              кнопками «Снять по таблице» / «Снять по запросу» на вкладке
              «Колонки» и переключателем на вкладке SQL. */}
          <Form.Item
            label="Select.sql"
            help="меняется на вкладках «Колонки» и «SQL» — тем, что вы там делаете"
          >
            <Space>
              {spec.src_mode === 'custom' ? (
                <Tag color="blue">ваш запрос, сохраняется как есть</Tag>
              ) : spec.src_mode === 'all' ? (
                <Tag color="orange">режим «вся таблица» (устарел)</Tag>
              ) : (
                <Tag>собирается из сопоставленных колонок</Tag>
              )}
            </Space>
          </Form.Item>
        </Col>
      </Row>

      {spec.src_mode === 'all' && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="Линия работает в режиме «вся таблица» (SELECT * FROM ведущей)"
          description={
            <Space direction="vertical" style={{ display: 'flex' }}>
              <Typography.Text>
                Колонки нигде не перечислены, поэтому сопоставление идёт по
                ПОЗИЦИИ, и добавленная кем-то колонка в ведущей сдвинет всё
                разом — без ошибки, просто данные лягут не туда. Режим оставлен
                только для чтения таких линий; переведите её на явный список.
              </Typography.Text>
              <Space>
                <Button
                  type="primary"
                  loading={snapping}
                  onClick={() => snapColumns('table')}
                >
                  Снять колонки из БД и перевести
                </Button>
                <Typography.Text type="secondary">
                  {spec.master_table} → {spec.slave_table}
                </Typography.Text>
              </Space>
              <ActionError error={snapError} />
            </Space>
          }
        />
      )}

      {entry.kind === 'once' && (
        <Form.Item
          label="Дополнять ведомую без очистки (append)"
          help="по умолчанию разовый перенос заливает таблицу заново"
        >
          <Switch checked={Boolean(spec.append)} onChange={(v) => patch({ append: v })} />
        </Form.Item>
      )}

      <Form.Item label="Комментарий (_doc)">
        <Input.TextArea
          autoSize={{ minRows: 2, maxRows: 8 }}
          value={spec.doc || ''}
          onChange={(e) => patch({ doc: e.target.value })}
        />
      </Form.Item>
    </Form>
  )

  const mapping = (
    <>
      <SpMappingEditor
        spec={spec}
        onChange={patchMapping}
        onRenameMaster={renameMasterColumn}
        onFixCase={fixColumnCase}
        onSnap={snapColumns}
        snapping={snapping || buildSql.loading}
      />
      <ActionError error={snapError} />
      <ActionError error={buildSql.error} />
    </>
  )

  const sql = (
    <Form layout="vertical">
      {/* Оба запроса правятся руками, и правка сразу видна на вкладке
          «Колонки» — как и наоборот. Линия справочника ОПИСАНА этими двумя
          файлами, так что колонки и SQL — два взгляда на одно и то же, а не
          «настоящее» и «только для чтения». Разбор идёт по уходу из поля:
          недописанный SELECT разбирать нечего.

          В режиме «выбранные колонки» Select собирает конструктор, поэтому
          ручная правка там будет затёрта следующим же изменением колонок — об
          этом сказано прямо, а рядом кнопка перевода в «своё SELECT», после
          которой запрос принадлежит человеку. */}
      {/* Один вопрос, а не выбор варианта: кто сейчас хозяин Select.sql. От
          ответа зависит ровно одно — перезапишет ли конструктор ваш текст,
          когда вы поправите колонки. Всё остальное работает одинаково: колонки
          читаются из запроса, запрос собирается из колонок, оба направления
          доступны в любой момент. */}
      {spec.src_mode !== 'custom' ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message="Select.sql сейчас собирает конструктор — из ведущей таблицы и сопоставления"
          description={
            <Space direction="vertical" style={{ display: 'flex' }}>
              <span>
                Правьте его свободно: чтобы текст закрепился за вами и больше не
                пересобирался, нажмите кнопку ниже — или просто снимите колонки
                кнопкой «Снять по запросу» на вкладке «Колонки», и запрос станет
                вашим сам.
              </span>
              <Button size="small" onClick={() => patch({ src_mode: 'custom' })}>
                Запрос мой, не пересобирать
              </Button>
            </Space>
          }
        />
      ) : (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message="Select.sql ваш — сохраняется как есть"
          description={
            <Space direction="vertical" style={{ display: 'flex' }}>
              <span>
                Имена колонок ведущей читаются из его псевдонимов: поправили
                запрос — колонки обновятся, нажали «Снять по запросу» — обновятся
                и типы с ключами. Вернуть сборку из колонок можно в любой момент.
              </span>
              <Popconfirm
                title="Собирать Select.sql из колонок?"
                description="Ваш текст запроса будет заменён сгенерированным по текущему сопоставлению."
                okText="Собирать"
                cancelText="Нет"
                onConfirm={() => patchMapping({ src_mode: 'table' })}
              >
                <Button size="small">Собирать из колонок</Button>
              </Popconfirm>
            </Space>
          }
        />
      )}

      <Form.Item
        label={`Select.sql${spec.select_sql ? ` — ${spec.select_sql}` : ''}`}
        help="имена колонок ведущей читаются отсюда — правка видна на вкладке «Колонки»"
      >
        <Input.TextArea
          autoSize={{ minRows: 5, maxRows: 20 }}
          style={{ fontFamily: 'monospace' }}
          value={spec.select_sql_text || ''}
          onChange={(e) => patch({ select_sql_text: e.target.value })}
          onBlur={(e) => reparseSql({ select_sql_text: e.target.value })}
        />
      </Form.Item>
      <Form.Item
        label={`Add.sql${spec.add_sql ? ` — ${spec.add_sql}` : ''}`}
        help="колонки ведомой читаются отсюда — правка видна на вкладке «Колонки»"
      >
        <Input.TextArea
          autoSize={{ minRows: 3, maxRows: 12 }}
          style={{ fontFamily: 'monospace' }}
          value={spec.add_sql_text || ''}
          onChange={(e) => patch({ add_sql_text: e.target.value })}
          onBlur={(e) => reparseSql({ add_sql_text: e.target.value })}
        />
      </Form.Item>
      <ActionError error={parseSql.error} />
    </Form>
  )

  const actions = (
    <Space direction="vertical" size="large" style={{ display: 'flex' }}>
      {/* Переключатель «участие в работе» писал фрагмент на диск сразу по
          щелчку — единственное место, где правка уходила в репозиторий мимо
          «Предпросмотра». Флаг disabled лежит в том же фрагменте, что и всё
          остальное, поэтому он стал обычным полем формы: правится здесь,
          попадает на диск кнопкой «Записать» и до неё виден в предпросмотре. */}
      <div>
        <Typography.Title level={5}>Участие в работе</Typography.Title>
        <Space>
          <Switch
            checked={!spec.disabled}
            onChange={(v) => patch({ disabled: !v })}
          />
          <span>
            {spec.disabled ? 'отключена — даг её пропускает' : 'включена'}
            {Boolean(spec.disabled) !== Boolean(entry.disabled) &&
              ' (правка в форме — на диске пока по-прежнему)'}
          </span>
        </Space>
      </div>

      <div>
        <Typography.Title level={5}>Перевести в другой тип</Typography.Title>
        <Typography.Paragraph type="secondary">
          {entry.kind === 'regular'
            ? 'Сейчас это регулярный справочник: обновляется по заданию (isokaudit = 0). Перевод в разовый снимет с него это ожидание — и линия переедет на соседнюю вкладку.'
            : 'Сейчас это разовый перенос: запускается руками. Перевод в регулярный подключит его к дагу справочников — и линия переедет на соседнюю вкладку.'}
        </Typography.Paragraph>
        <Popconfirm
          title="Перевести линию?"
          okText="Перевести"
          cancelText="Нет"
          onConfirm={() =>
            move
              .run({
                key: entry.key,
                from_kind: entry.kind,
                to_kind: entry.kind === 'regular' ? 'once' : 'regular',
              })
              .then((r) => r && onChanged?.())
          }
        >
          <Button loading={move.loading}>
            → {entry.kind === 'regular' ? 'в разовый перенос' : 'в справочники'}
          </Button>
        </Popconfirm>
        <ActionError error={move.error} />
      </div>

      {entry.kind === 'regular' && (
        <div>
          <Typography.Title level={5}>Аудитный триггер ведущей</Typography.Title>
          <Typography.Paragraph type="secondary">
            Он ставит <Typography.Text code>isokaudit = 0</Typography.Text> в etl_jobs —
            это и есть сигнал «перенеси справочник». Без него линия не обновится
            никогда и ни одной ошибки при этом не выдаст.
          </Typography.Paragraph>
          <Button
            loading={auditTrigger.loading}
            disabled={!spec.master_table}
            onClick={() =>
              auditTrigger
                .run({
                  db: spec.db_master,
                  table: spec.master_table,
                  dependence: spec.dependence || spec.master_label,
                })
                .then((r) => r && setDdl(r))
            }
          >
            Показать DDL
          </Button>
          <ActionError error={auditTrigger.error} />
        </div>
      )}

      <div>
        <Typography.Title level={5} type="danger">
          Удалить насовсем
        </Typography.Title>
        <Space>
          <Button
            loading={delTargets.loading}
            onClick={() =>
              delTargets
                .run({ kind: entry.kind, key: entry.key })
                .then((r) => r && setTargets(r))
            }
          >
            Показать, что будет удалено
          </Button>
          <Popconfirm
            title="Удалить линию насовсем?"
            okText="⚠️ Да, удалить"
            okButtonProps={{ danger: true }}
            cancelText="Нет"
            disabled={!targets}
            onConfirm={() =>
              remove
                .run({ kind: entry.kind, key: entry.key })
                .then((r) => r && onChanged?.())
            }
          >
            <Button danger disabled={!targets} loading={remove.loading}>
              Удалить
            </Button>
          </Popconfirm>
        </Space>
        <ActionError error={delTargets.error || remove.error} />
        {targets && (
          <Alert
            style={{ marginTop: 8 }}
            type="warning"
            showIcon
            message="Будет удалено"
            description={
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                <li>
                  фрагмент: <Typography.Text code>{targets.fragment}</Typography.Text>
                </li>
                <li>
                  каталог SQL: <Typography.Text code>{targets.sql_dir || '—'}</Typography.Text>
                  {targets.sql_dir_shared && (
                    <b> — НЕ удаляется: им пользуется ещё одна линия</b>
                  )}
                </li>
              </ul>
            }
          />
        )}
      </div>
    </Space>
  )

  return (
    <Space direction="vertical" size="middle" style={{ display: 'flex' }}>
      <Card
        title={
          <Space wrap>
            <b>{entry.key || (spec.master_label ? previewKey : 'Новая линия')}</b>
            <Tag>{KINDS[entry.kind]}</Tag>
            {isNew && <Tag color="green">создаётся</Tag>}
            <Tag>{spec.db_master} → {spec.db_slave}</Tag>
            {dirty && <Tag color="orange">есть несохранённые правки</Tag>}
          </Space>
        }
        extra={
          <Space>
            <Popconfirm
              title="Отменить все правки?"
              okText="Отменить правки"
              cancelText="Нет"
              disabled={!dirty}
              onConfirm={() => setSpec(JSON.parse(original))}
            >
              <Button icon={<UndoOutlined />} disabled={!dirty}>
                Отменить
              </Button>
            </Popconfirm>
            <Button
              type="primary"
              loading={preview.loading}
              disabled={keyTaken}
              onClick={() => {
                write.reset()
                preview.run({ ...spec, kind: entry.kind })
              }}
            >
              Предпросмотр
            </Button>
          </Space>
        }
      >
        <Tabs
          items={[
            { key: 'set', label: 'Настройки', children: settings },
            { key: 'map', label: `Колонки (${(spec.pairs || []).length})`, children: mapping },
            { key: 'sql', label: 'SQL', children: sql },
            {
              key: 'act',
              label: 'Действия',
              // Перевод между типами и удаление относятся к тому, что уже лежит
              // на диске: у несозданной линии их адресата попросту нет.
              children: isNew ? (
                <Alert
                  type="info"
                  showIcon
                  message="Линия ещё не создана"
                  description="Перевод между типами, удаление и DDL аудитного триггера появятся, как только вы её запишете."
                />
              ) : (
                actions
              ),
            },
          ]}
        />
        <ActionError error={preview.error} onClose={preview.reset} />
      </Card>

      {preview.result && (
        <FilesPreview
          files={preview.result.files}
          unchanged={preview.result.unchanged}
          created={preview.result.created}
          busy={write.loading}
          error={write.error}
          written={write.result}
          onWrite={(chosen) =>
            write
              .run({
                files: chosen.map((f) => [f.path, f.content]),
                // У НОВОЙ линии перезапись запрещена: если файл с таким именем
                // уже есть, значит ключ занят — лучше отказ, чем молча съеденная
                // чужая линия.
                overwrite: !isNew,
              })
              .then((r) => {
                if (!r) return
                setOriginal(JSON.stringify(spec))
                // onCreated сам обновляет список и выбирает новую линию,
                // поэтому второй раз дёргать onChanged незачем
                if (isNew) onCreated?.(preview.result.key)
                else onChanged?.()
              })
          }
        />
      )}

      <Modal
        open={Boolean(ddl)}
        onCancel={() => setDdl(null)}
        footer={<Button onClick={() => setDdl(null)}>Закрыть</Button>}
        width={900}
        title={ddl ? `Аудитный триггер: ${ddl.name}` : ''}
      >
        {ddl && <pre style={{ maxHeight: '55vh', overflow: 'auto' }}>{ddl.text}</pre>}
      </Modal>
    </Space>
  )
}
