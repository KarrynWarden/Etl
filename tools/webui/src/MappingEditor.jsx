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
import { DeleteOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import ComboBox from './ComboBox'
import SnapReview from './SnapReview'

// Правка сопоставления колонок существующей линии.
//
// Порядок здесь — не оформление, а смысл: перенос читает строку ведущей и
// пишет её в ведомую ПО ПОЗИЦИЯМ структур, а не по именам. Пара с разными
// именами (GROUPPCODE / groupcode, DVISIT / dvizit, idrw / prvdirid) — норма и
// встречается в боевых линиях; сдвиг на одну строку означал бы, что данные
// поедут не в те колонки. Поэтому строка таблицы — это ПАРА, а не колонка.
//
// Типы: список частых собран из etlFolder/structures/**/*.json, но поле
// свободное — редкий тип вписывается руками. Тип сверяется с
// information_schema / all_tab_columns ДОСЛОВНО, поэтому писать надо ровно
// так, как его отдаёт БД.
const TYPES = {
  Orcl: ['NUMBER', 'VARCHAR2', 'DATE', 'CLOB', 'CHAR'],
  Post: ['numeric', 'character varying', 'date', 'timestamp without time zone',
         'text', 'integer', 'smallint', 'bigint'],
}

const nameOf = (c) => c?.column_name || c?.COLUMN_NAME || ''
const typeOf = (c) => c?.data_type || c?.DATA_TYPE || ''
const scaleOf = (c) => {
  const v = c?.data_scale ?? c?.DATA_SCALE
  return v === null || v === undefined ? '' : String(v)
}
const isPk = (c) => Boolean(c?.is_primary_key || c?.IS_PRIMARY_KEY)
const typeText = (c) => (scaleOf(c) ? `${typeOf(c)}(${scaleOf(c)})` : typeOf(c))

// Ключи полей у сторон разные: Oracle отдаёт ВЕРХНИЙ регистр имён ключей,
// Postgres — нижний. Правим тот, который в объекте реально есть.
function patchCol(col, changes) {
  const out = { ...col }
  const upper = 'COLUMN_NAME' in out
  const map = upper
    ? { name: 'COLUMN_NAME', type: 'DATA_TYPE', scale: 'DATA_SCALE', pk: 'IS_PRIMARY_KEY' }
    : { name: 'column_name', type: 'data_type', scale: 'data_scale', pk: 'is_primary_key' }
  for (const [k, v] of Object.entries(changes)) out[map[k]] = v
  return out
}

function emptyCol(db, name) {
  return db === 'Orcl'
    ? { COLUMN_NAME: name, DATA_TYPE: 'VARCHAR2', DATA_SCALE: null, IS_PRIMARY_KEY: null }
    : { column_name: name, data_type: 'text', data_scale: null, is_primary_key: null }
}

const byName = (cols) => {
  const m = new Map()
  cols.forEach((c) => m.set(nameOf(c), c))
  return m
}

// Отличия снятого из БД от того, что записано у линии. Возвращает плоский
// список решений — по одному на каждое отличие (см. SnapReview).
function collectChanges(spec, dbMaster, dbSlave) {
  const out = []
  const push = (kind, name, before, after, payload) =>
    out.push({ id: `${kind}:${name}`, kind, name, before, after, payload })

  const sides = [
    ['master', spec.master_cols || [], dbMaster],
    ['slave', spec.slave_cols || [], dbSlave],
  ]
  for (const [side, current, fresh] of sides) {
    if (!fresh) continue
    const now = byName(current)
    const db = byName(fresh)
    for (const col of fresh)
      if (!now.has(nameOf(col)))
        push(`${side}_added`, nameOf(col), undefined, typeText(col), col)
    for (const col of current)
      if (!db.has(nameOf(col)))
        push(`${side}_removed`, nameOf(col), typeText(col), undefined, col)
    for (const col of current) {
      const other = db.get(nameOf(col))
      if (!other) continue
      if (typeText(col) !== typeText(other))
        push(`${side}_type`, nameOf(col), typeText(col), typeText(other), other)
      if (side === 'master' && isPk(col) !== isPk(other))
        push('master_pk', nameOf(col), isPk(col) ? 'ключ' : 'не ключ',
             isPk(other) ? 'ключ' : 'не ключ', other)
    }
  }
  return out
}

export default function MappingEditor({ spec, onChange }) {
  const [newMaster, setNewMaster] = useState('')
  const [newSlave, setNewSlave] = useState('')
  const [filter, setFilter] = useState('all')
  const [allowReuse, setAllowReuse] = useState(false)
  const [review, setReview] = useState(null)      // {changes, master, slave}
  const [decisions, setDecisions] = useState({})
  const [nameError, setNameError] = useState(null)

  const snapMaster = useAction(api.snapStructure)
  const snapQuery = useAction(api.snapQueryStructure)
  const snapSlave = useAction(api.snapStructure)
  const match = useAction(api.match)

  const master = spec.master_cols || []
  const slave = spec.slave_cols || []
  const pairs = spec.pairs || []

  const masterBy = useMemo(() => byName(master), [master])
  const slaveBy = useMemo(() => byName(slave), [slave])
  const slaveNames = useMemo(() => slave.map(nameOf), [slave])
  const usedSlaves = useMemo(
    () => new Set(pairs.map(([, s]) => s).filter(Boolean)),
    [pairs],
  )

  const setPair = (i, slaveName) =>
    onChange({ pairs: pairs.map((p, j) => (j === i ? [p[0], slaveName || null] : p)) })

  const setMasterCol = (name, changes) =>
    onChange({
      master_cols: master.map((c) => (nameOf(c) === name ? patchCol(c, changes) : c)),
    })

  const setSlaveCol = (name, changes) =>
    onChange({
      slave_cols: slave.map((c) => (nameOf(c) === name ? patchCol(c, changes) : c)),
    })

  // Переименование колонки ведущей правит СРАЗУ ДВА места: саму структуру и
  // левую половину пары. Нужно это, когда в SELECT поменяли псевдоним: имена в
  // структуре ведущей — это именно псевдонимы запроса, и разойтись им нельзя,
  // иначе рантайм не соберёт строку.
  const renameMaster = (from, event) => {
    const name = event.target.value.trim()
    if (name === from) return
    // Отказ обязан быть виден и в поле тоже: поле здесь неуправляемое (иначе
    // недописанное имя перекраивало бы пары на каждой букве), и без возврата
    // текста на экране осталось бы имя, которого в линии нет.
    const reject = (why) => {
      setNameError(why)
      event.target.value = from
    }
    if (!name) return reject(`Пустое имя колонке не годится — ${from} оставлен как был.`)
    if (masterBy.has(name))
      return reject(
        `Колонка ${name} у ведущей уже есть: два одинаковых имени сделали бы ` +
          `сопоставление неоднозначным. ${from} оставлен как был.`,
      )
    setNameError(null)
    onChange({
      master_cols: master.map((c) => (nameOf(c) === from ? patchCol(c, { name }) : c)),
      pairs: pairs.map(([m, s]) => (m === from ? [name, s] : [m, s])),
    })
  }

  const removeRow = (i) => {
    const [m] = pairs[i]
    onChange({
      pairs: pairs.filter((_p, j) => j !== i),
      master_cols: master.filter((c) => nameOf(c) !== m),
    })
  }

  const addMaster = () => {
    const name = newMaster.trim()
    if (!name || masterBy.has(name)) return
    onChange({
      master_cols: [...master, emptyCol(spec.db_master, name)],
      pairs: [...pairs, [name, null]],
    })
    setNewMaster('')
  }

  const addSlave = () => {
    const name = newSlave.trim()
    if (!name || slaveBy.has(name)) return
    onChange({ slave_cols: [...slave, emptyCol(spec.db_slave, name)] })
    setNewSlave('')
  }

  // ── снять структуры из БД ────────────────────────────────────────────────
  // Ведущая снимается ПО ТЕКУЩЕМУ ТЕКСТУ ЗАПРОСА из формы, а не по тому, что
  // лежит в файле: правка SELECT и правка структуры — это одна работа, и
  // делить её на два круга «сохрани → сними → сохрани» незачем. Имена колонок
  // при своём SELECT — это его псевдонимы, ровно их и видит рантайм.
  const snapBoth = async () => {
    const sql = (spec.select_sql_text || '').trim()
    const m = sql
      ? await snapQuery.run({ db: spec.db_master, sql })
      : await snapMaster.run({ db: spec.db_master, table: spec.table_master })
    if (!m) return
    const s = await snapSlave.run({ db: spec.db_slave, table: spec.table_slave })
    if (!s) return
    const changes = collectChanges(spec, m.columns, s.columns)
    setDecisions({})
    setReview({ changes, master: m.columns, slave: s.columns })
  }

  const decide = (id, value) =>
    setDecisions((prev) =>
      id === '__all__'
        ? Object.fromEntries((review?.changes || []).map((c) => [c.id, value]))
        : { ...prev, [id]: value },
    )

  // Применяем ТОЛЬКО принятое. Всё остальное — включая пары — остаётся как
  // было; в этом весь смысл разбора.
  const applyReview = async () => {
    const taken = (review.changes || []).filter((c) => decisions[c.id] === 'yes')
    let cols = [...master]
    let sCols = [...slave]
    let next = pairs.map((p) => [...p])

    for (const ch of taken) {
      if (ch.kind === 'master_removed') {
        cols = cols.filter((c) => nameOf(c) !== ch.name)
        next = next.filter(([m]) => m !== ch.name)
      } else if (ch.kind === 'slave_removed') {
        sCols = sCols.filter((c) => nameOf(c) !== ch.name)
        next = next.map(([m, s]) => [m, s === ch.name ? null : s])
      } else if (ch.kind === 'slave_added') {
        sCols = [...sCols, ch.payload]
      } else if (ch.kind === 'master_type' || ch.kind === 'master_pk') {
        cols = cols.map((c) =>
          nameOf(c) === ch.name
            ? patchCol(c, {
                type: typeOf(ch.payload),
                scale: ch.payload.data_scale ?? ch.payload.DATA_SCALE ?? null,
                pk: isPk(ch.payload) ? 'Primary Key' : null,
              })
            : c,
        )
      } else if (ch.kind === 'slave_type') {
        sCols = sCols.map((c) =>
          nameOf(c) === ch.name
            ? patchCol(c, {
                type: typeOf(ch.payload),
                scale: ch.payload.data_scale ?? ch.payload.DATA_SCALE ?? null,
              })
            : c,
        )
      }
    }

    // Новые колонки ведущей добавляем последними и в порядке снимка: только им
    // нужен подбор пары, и подбирать его есть смысл лишь среди СВОБОДНЫХ
    // колонок ведомой — занятые уже кем-то расставлены руками.
    const added = taken.filter((c) => c.kind === 'master_added')
    if (added.length) {
      const busy = new Set(next.map(([, s]) => s).filter(Boolean))
      const free = sCols.filter((c) => !busy.has(nameOf(c)))
      const suggestion = free.length
        ? await match.run({ master_cols: added.map((c) => c.payload), slave_cols: free })
        : null
      added.forEach((ch, i) => {
        cols = [...cols, ch.payload]
        next = [...next, [ch.name, suggestion?.suggestions?.[i] || null]]
      })
    }

    onChange({ master_cols: cols, slave_cols: sCols, pairs: next })
    setReview(null)
  }

  const rows = pairs
    .map(([m, s], i) => ({ i, m, s }))
    .filter((r) =>
      filter === 'matched' ? Boolean(r.s) : filter === 'unmatched' ? !r.s : true,
    )

  const unmatched = pairs.filter(([, s]) => !s).length
  const pkMismatch = pairs.filter(
    ([m, s]) => isPk(masterBy.get(m)) !== isPk(slaveBy.get(s)),
  ).length
  const busy = snapMaster.loading || snapSlave.loading || snapQuery.loading || match.loading
  const orphanSlaves = slaveNames.filter((n) => !usedSlaves.has(n))
  const doubled = slaveNames.filter(
    (n) => pairs.filter(([, s]) => s === n).length > 1,
  )
  const usingSql = Boolean((spec.select_sql_text || '').trim())

  // Список для колонки ведомой: по умолчанию только СВОБОДНЫЕ плюс своя
  // текущая. Одна колонка ведущей в две колонки ведомой — случай редкий и
  // обычно решается псевдонимом в SELECT, а вот случайно занять чужую пару из
  // общего списка легко: имена похожи, строк полсотни.
  const slaveOptions = (mine) =>
    allowReuse ? slaveNames : slaveNames.filter((n) => n === mine || !usedSlaves.has(n))

  return (
    <>
      <Space wrap style={{ marginBottom: 12 }}>
        <Button icon={<ReloadOutlined />} loading={busy} onClick={snapBoth}>
          Снять структуры из БД
        </Button>
        <Typography.Text type="secondary">
          {spec.db_master} {usingSql ? 'по тексту SELECT из формы' : spec.table_master} →{' '}
          {spec.db_slave} {spec.table_slave}
        </Typography.Text>
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

      <ActionError error={snapMaster.error} />
      <ActionError error={snapQuery.error} />
      <ActionError error={snapSlave.error} />
      <ActionError error={match.error} />

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
      {unmatched > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Без пары осталось колонок ведущей: ${unmatched}`}
          description="Такие колонки в линию не попадут — ни в структуры, ни в перенос."
        />
      )}
      {doubled.length > 0 && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Одна колонка ведомой занята несколькими парами: ${[...new Set(doubled)].join(', ')}`}
          description="В ведомую поедет последняя из них, остальные значения потеряются. Почти всегда это промах, а не замысел."
        />
      )}
      {pkMismatch > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Признак первичного ключа расходится у ${pkMismatch} пар`}
          description="Ключ задаёт ведущая, ведомая колонка той же позиции помечается так же. При записи это выровняется по ведущей."
        />
      )}
      {orphanSlaves.length > 0 && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Колонки ведомой без пары: ${orphanSlaves.join(', ')}`}
          description="Они останутся в таблице, но ETL их не заполняет."
        />
      )}

      <Table
        size="small"
        bordered
        pagination={false}
        scroll={{ y: 420 }}
        rowKey={(r) => r.i}
        dataSource={rows}
        columns={[
          { title: '#', dataIndex: 'i', width: 44, render: (v) => v + 1 },
          {
            title: `Ведущая (${spec.db_master})`,
            width: '32%',
            render: (_v, r) => {
              const col = masterBy.get(r.m)
              return (
                <Space direction="vertical" size={2} style={{ width: '100%' }}>
                  {/* Имя правится прямо здесь: поменяли псевдоним в SELECT —
                      меняете его же в структуре, одним заходом и одним
                      коммитом. defaultValue, а не value: пока имя дописывают,
                      промежуточный вариант не должен перекраивать пары. */}
                  <Input
                    size="small"
                    defaultValue={r.m}
                    key={r.m}
                    style={{ fontFamily: 'monospace' }}
                    onBlur={(e) => renameMaster(r.m, e)}
                    onPressEnter={(e) => renameMaster(r.m, e)}
                  />
                  <Space size={4}>
                    <ComboBox
                      style={{ width: 190 }}
                      size="small"
                      value={typeOf(col)}
                      options={TYPES[spec.db_master] || []}
                      onChange={(v) => setMasterCol(r.m, { type: v })}
                    />
                    <Tooltip title="DATA_SCALE — сверяется с БД дословно">
                      <Input
                        size="small"
                        style={{ width: 60 }}
                        placeholder="scale"
                        value={scaleOf(col)}
                        onChange={(e) =>
                          setMasterCol(r.m, {
                            scale: e.target.value === '' ? null : Number(e.target.value),
                          })
                        }
                      />
                    </Tooltip>
                  </Space>
                </Space>
              )
            },
          },
          {
            title: 'PK',
            width: 60,
            render: (_v, r) => (
              <Checkbox
                checked={isPk(masterBy.get(r.m))}
                onChange={(e) =>
                  setMasterCol(r.m, { pk: e.target.checked ? 'Primary Key' : null })
                }
              />
            ),
          },
          {
            title: `Ведомая (${spec.db_slave})`,
            width: '32%',
            render: (_v, r) => {
              const col = slaveBy.get(r.s)
              return (
                <Space direction="vertical" size={2} style={{ width: '100%' }}>
                  <ComboBox
                    style={{ width: '100%' }}
                    size="small"
                    value={r.s || ''}
                    options={slaveOptions(r.s)}
                    placeholder="— нет пары —"
                    onChange={(v) => setPair(r.i, v)}
                  />
                  {col && (
                    <Space size={4}>
                      <ComboBox
                        style={{ width: 190 }}
                        size="small"
                        value={typeOf(col)}
                        options={TYPES[spec.db_slave] || []}
                        onChange={(v) => setSlaveCol(r.s, { type: v })}
                      />
                      <Input
                        size="small"
                        style={{ width: 60 }}
                        placeholder="scale"
                        value={scaleOf(col)}
                        onChange={(e) =>
                          setSlaveCol(r.s, {
                            scale: e.target.value === '' ? null : Number(e.target.value),
                          })
                        }
                      />
                    </Space>
                  )}
                </Space>
              )
            },
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
                title="Убрать колонку ведущей из линии?"
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
        <Space.Compact>
          <Input
            size="small"
            style={{ width: 200 }}
            placeholder="имя колонки ведущей"
            value={newMaster}
            onChange={(e) => setNewMaster(e.target.value)}
            onPressEnter={addMaster}
          />
          <Button size="small" icon={<PlusOutlined />} onClick={addMaster}>
            колонка ведущей
          </Button>
        </Space.Compact>
        <Space.Compact>
          <Input
            size="small"
            style={{ width: 200 }}
            placeholder="имя колонки ведомой"
            value={newSlave}
            onChange={(e) => setNewSlave(e.target.value)}
            onPressEnter={addSlave}
          />
          <Button size="small" icon={<PlusOutlined />} onClick={addSlave}>
            колонка ведомой
          </Button>
        </Space.Compact>
        <Typography.Text type="secondary">
          сопоставлено {pairs.length - unmatched} из {master.length}
        </Typography.Text>
      </Space>

      <SnapReview
        open={Boolean(review)}
        changes={review?.changes}
        decisions={decisions}
        onDecide={decide}
        onApply={applyReview}
        onCancel={() => setReview(null)}
      />
    </>
  )
}
