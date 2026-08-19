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
import { DeleteOutlined, FontSizeOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons'

import ComboBox from './ComboBox'
import { toDbCase, matchesDbCase } from './dbCase'

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

export default function SpMappingEditor({
  spec,
  onChange,
  onSnap,
  snapping,
  snapDisabled,
}) {
  const [filter, setFilter] = useState('all')
  const [allowReuse, setAllowReuse] = useState(false)
  const [newSlave, setNewSlave] = useState('')
  const [nameError, setNameError] = useState(null)

  const pairs = spec.pairs || []
  const slaveNames = useMemo(() => (spec.slave_cols || []).map(nameOf), [spec.slave_cols])
  const masterNames = useMemo(() => pairs.map(([m]) => m), [pairs])
  const usedSlaves = useMemo(
    () => new Set(pairs.map(([, s]) => s).filter(Boolean)),
    [pairs],
  )

  // Имя ведущей попадает в СГЕНЕРИРОВАННЫЙ SELECT только в режиме «выбранные
  // колонки». В режиме «своё SELECT» запрос принадлежит человеку, и менять там
  // регистр имени, не тронув сам запрос, — верный способ развести структуру с
  // тем, что вернёт драйвер.
  const masterEditable = spec.src_mode !== 'custom'

  const setPair = (i, m, s) =>
    onChange({ pairs: pairs.map((p, j) => (j === i ? [m, s] : p)) })

  const renameMaster = (i, event) => {
    const name = event.target.value.trim()
    const from = pairs[i][0]
    if (name === from) return
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
    setPair(i, name, pairs[i][1])
  }

  const renameSlave = (from, event) => {
    const name = event.target.value.trim()
    if (name === from) return
    if (!name || slaveNames.includes(name)) {
      setNameError(
        !name
          ? `Пустое имя колонке не годится — ${from} оставлен как был.`
          : `Колонка ${name} у ведомой уже есть — в INSERT имена не повторяются.`,
      )
      event.target.value = from
      return
    }
    setNameError(null)
    onChange({
      slave_cols: (spec.slave_cols || []).map((c) =>
        nameOf(c) === from ? { ...(typeof c === 'string' ? {} : c), column_name: name } : c,
      ),
      pairs: pairs.map(([m, s]) => [m, s === from ? name : s]),
    })
  }

  // Приведение регистра сразу у всех имён — по колонке за раз это полсотни
  // нажатий. Ведущая правится только там, где запрос собирается конструктором.
  const offCase = [
    ...(masterEditable ? masterNames.filter((n) => n && !matchesDbCase(n, spec.db_master)) : []),
    ...slaveNames.filter((n) => n && !matchesDbCase(n, spec.db_slave)),
  ]
  const fixCase = () => {
    setNameError(null)
    onChange({
      slave_cols: (spec.slave_cols || []).map((c) => ({
        ...(typeof c === 'string' ? {} : c),
        column_name: toDbCase(nameOf(c), spec.db_slave),
      })),
      pairs: pairs.map(([m, s]) => [
        masterEditable && m ? toDbCase(m, spec.db_master) : m,
        s ? toDbCase(s, spec.db_slave) : s,
      ]),
    })
  }

  const removeRow = (i) => onChange({ pairs: pairs.filter((_p, j) => j !== i) })

  const addSlave = () => {
    const name = newSlave.trim()
    if (!name || slaveNames.includes(name)) return
    onChange({ slave_cols: [...(spec.slave_cols || []), { column_name: name }] })
    setNewSlave('')
  }

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
      <Space wrap style={{ marginBottom: 12 }}>
        <Button icon={<ReloadOutlined />} loading={snapping} disabled={snapDisabled} onClick={onSnap}>
          Снять колонки из БД
        </Button>
        <Tooltip
          title={
            offCase.length
              ? `Привести к регистру диалекта: ведущая ${spec.db_master}, ведомая ${spec.db_slave}`
              : masterEditable
                ? 'Регистр всех имён уже соответствует диалекту'
                : 'Имена ведущей приходят из вашего SELECT — их регистр здесь не трогается; проверяется только ведомая'
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
          description={
            spec.src_mode === 'custom'
              ? 'Впишите запрос на вкладке SQL — колонки разберутся из него сами, — или снимите их из БД.'
              : 'Заполните обе таблицы на вкладке «Настройки» и нажмите «Снять колонки из БД».'
          }
        />
      )}
      {spec.src_mode === 'custom' && Boolean(pairs.length) && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="Для своего SELECT сопоставлены должны быть ВСЕ колонки"
          description="Вставка идёт по порядку колонок запроса, пропуски недопустимы. Имена ведущей здесь — псевдонимы запроса: правьте их на вкладке SQL, оттуда они и читаются."
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
          message={`Колонки ведомой без пары: ${orphanSlaves.join(', ')}`}
          description="Они останутся в таблице, но перенос их не заполняет."
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
            render: (_v, r) =>
              masterEditable ? (
                <Input
                  size="small"
                  key={r.m}
                  defaultValue={r.m}
                  style={{ fontFamily: 'monospace' }}
                  onBlur={(e) => renameMaster(r.i, e)}
                  onPressEnter={(e) => renameMaster(r.i, e)}
                />
              ) : (
                <Typography.Text code>{r.m}</Typography.Text>
              ),
          },
          {
            title: `Ведомая (${spec.db_slave})`,
            render: (_v, r) => (
              <ComboBox
                style={{ width: '100%' }}
                size="small"
                value={r.s || ''}
                options={slaveOptions(r.s)}
                placeholder="— нет пары —"
                onChange={(v) => setPair(r.i, r.m, v || null)}
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
        {masterEditable && (
          <Button size="small" icon={<PlusOutlined />} onClick={addPair}>
            строку сопоставления
          </Button>
        )}
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
          сопоставлено {pairs.length - unmatched} из {pairs.length}
        </Typography.Text>
      </Space>

      {/* Переименование колонки ведомой — это правка INSERT, поэтому список
          имён редактируемый и здесь: в паре выбирается имя, а само имя живёт
          тут. */}
      {Boolean(slaveNames.length) && (
        <div style={{ marginTop: 16 }}>
          <Typography.Text type="secondary">
            Колонки ведомой (правка имени меняет INSERT):
          </Typography.Text>
          <Space wrap style={{ marginTop: 6 }}>
            {(spec.slave_cols || []).map((c) => (
              <Input
                key={nameOf(c)}
                size="small"
                style={{ width: 170, fontFamily: 'monospace' }}
                defaultValue={nameOf(c)}
                onBlur={(e) => renameSlave(nameOf(c), e)}
                onPressEnter={(e) => renameSlave(nameOf(c), e)}
              />
            ))}
          </Space>
        </div>
      )}
    </>
  )
}
