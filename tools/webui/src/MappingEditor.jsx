import { useState } from 'react'
import {
  Alert,
  Button,
  Checkbox,
  Input,
  Popconfirm,
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

// Правка сопоставления колонок существующей линии.
//
// Порядок здесь — не оформление, а смысл: перенос читает строку ведущей и
// пишет её в ведомую ПО ПОЗИЦИЯМ структур, а не по именам. Пара с разными
// именами (GROUPPCODE / groupcode, DVISIT / dvizit) — норма и встречается в
// боевых линиях; сдвиг на одну строку означал бы, что данные поедут не в те
// колонки. Поэтому строка таблицы — это ПАРА, а не колонка.
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

export default function MappingEditor({ spec, onChange }) {
  const [newMaster, setNewMaster] = useState('')
  const [newSlave, setNewSlave] = useState('')

  const snapMaster = useAction(api.snapStructure)
  const snapSlave = useAction(api.snapStructure)
  const match = useAction(api.match)

  const master = spec.master_cols || []
  const slave = spec.slave_cols || []
  const pairs = spec.pairs || []

  const byName = (cols) => {
    const m = new Map()
    cols.forEach((c) => m.set(nameOf(c), c))
    return m
  }
  const masterBy = byName(master)
  const slaveBy = byName(slave)
  const slaveNames = slave.map(nameOf)

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

  const removeRow = (i) => {
    const [m] = pairs[i]
    onChange({
      pairs: pairs.filter((_p, j) => j !== i),
      master_cols: master.filter((c) => nameOf(c) !== m),
    })
  }

  const addMaster = () => {
    const name = newMaster.trim()
    if (!name) return
    onChange({
      master_cols: [...master, emptyCol(spec.db_master, name)],
      pairs: [...pairs, [name, null]],
    })
    setNewMaster('')
  }

  const addSlave = () => {
    const name = newSlave.trim()
    if (!name) return
    onChange({ slave_cols: [...slave, emptyCol(spec.db_slave, name)] })
    setNewSlave('')
  }

  // Снять структуры заново из БД. Это ЗАМЕНА, а не дополнение: типы, признак
  // PK и состав колонок берутся из базы, ручные правки в них пропадают.
  // Сопоставление после этого подбирается по именам, поэтому пары с РАЗНЫМИ
  // именами придётся расставить снова — о чём и предупреждаем до нажатия.
  const snapBoth = async () => {
    const m = await snapMaster.run({
      db: spec.db_master,
      table: spec.table_master,
    })
    if (!m) return
    const s = await snapSlave.run({ db: spec.db_slave, table: spec.table_slave })
    if (!s) return
    const suggestion = await match.run({
      master_cols: m.columns,
      slave_cols: s.columns,
    })
    // прежнее сопоставление важнее автоподбора: имя, которое человек уже
    // связал руками, менять не за что
    const previous = new Map(pairs)
    onChange({
      master_cols: m.columns,
      slave_cols: s.columns,
      pairs: m.columns.map((c, i) => {
        const name = nameOf(c)
        const kept = previous.get(name)
        const still = kept && s.columns.some((sc) => nameOf(sc) === kept)
        return [name, still ? kept : suggestion?.suggestions?.[i] || null]
      }),
    })
  }

  const unmatched = pairs.filter(([, s]) => !s).length
  const pkMismatch = pairs.filter(
    ([m, s]) => isPk(masterBy.get(m)) !== isPk(slaveBy.get(s)),
  ).length
  const busy = snapMaster.loading || snapSlave.loading || match.loading
  const orphanSlaves = slaveNames.filter((n) => !pairs.some(([, s]) => s === n))

  return (
    <>
      <Space wrap style={{ marginBottom: 12 }}>
        <Popconfirm
          title="Снять структуры заново?"
          description={
            <div style={{ maxWidth: 380 }}>
              Состав колонок, типы и признак первичного ключа будут взяты из БД —
              ручные правки в них пропадут. Сопоставление сохранится там, где имена
              совпадут; остальное подберётся заново.
            </div>
          }
          okText="Снять из БД"
          cancelText="Нет"
          onConfirm={snapBoth}
        >
          <Button icon={<ReloadOutlined />} loading={busy}>
            Снять структуры из БД
          </Button>
        </Popconfirm>
        <Typography.Text type="secondary">
          {spec.db_master} {spec.table_master} → {spec.db_slave} {spec.table_slave}
        </Typography.Text>
      </Space>

      <ActionError error={snapMaster.error} />
      <ActionError error={snapSlave.error} />
      <ActionError error={match.error} />

      {unmatched > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message={`Без пары осталось колонок ведущей: ${unmatched}`}
          description="Такие колонки в линию не попадут — ни в структуры, ни в перенос."
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
        rowKey={(_r, i) => i}
        dataSource={pairs.map(([m, s], i) => ({ i, m, s }))}
        columns={[
          { title: '#', dataIndex: 'i', width: 44, render: (v) => v + 1 },
          {
            title: `Ведущая (${spec.db_master})`,
            width: '32%',
            render: (_v, r) => {
              const col = masterBy.get(r.m)
              return (
                <Space direction="vertical" size={2} style={{ width: '100%' }}>
                  <Typography.Text code>{r.m}</Typography.Text>
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
                    options={slaveNames}
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
          сопоставлено {pairs.filter(([, s]) => s).length} из {master.length}
        </Typography.Text>
      </Space>
    </>
  )
}
