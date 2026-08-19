import { useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Form,
  Input,
  Radio,
  Row,
  Select,
  Space,
  Steps,
  Table,
  Tag,
  Typography,
} from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import ComboBox from './ComboBox'
import FilesPreview from './FilesPreview'
import TableNameInput from './TableNameInput'

const DBS = [
  { value: 'Orcl', label: 'Oracle' },
  { value: 'Post', label: 'PostgreSQL' },
]

const colName = (c) => c?.column_name || c?.COLUMN_NAME || ''
const colType = (c) => c?.data_type || c?.DATA_TYPE || ''

// Новая линия с нуля.
//
// Разбито на шаги не для красоты: снятие структур ходит в БД и занимает
// секунды, а сопоставление колонок имеет смысл только после него. В прежнем
// интерфейсе всё лежало одной длинной формой, и было неочевидно, что сначала
// надо нажать «Снять структуры», а ошибка этого шага уезжала в консоль внизу.
export default function NewLinePage({ onCreated }) {
  const [step, setStep] = useState(0)
  const [form, setForm] = useState({
    db_master: 'Orcl',
    db_slave: 'Post',
    table_master: '',
    table_slave: '',
    mode: 'iud',
    source: 'table',
    select_sql_text: '',
  })
  const [masterCols, setMasterCols] = useState([])
  const [slaveCols, setSlaveCols] = useState([])
  const [pairs, setPairs] = useState([])
  const [spec, setSpec] = useState(null)

  const snapMaster = useAction(api.snapStructure)
  const snapQuery = useAction(api.snapQueryStructure)
  const snapSlave = useAction(api.snapStructure)
  const match = useAction(api.match)
  const defaults = useAction(api.defaults)
  const preview = useAction(api.preview)
  const write = useAction(api.write)

  const patch = (c) => setForm((p) => ({ ...p, ...c }))
  const slaveNames = useMemo(() => slaveCols.map(colName), [slaveCols])

  const snapAll = async () => {
    const m =
      form.source === 'sql'
        ? await snapQuery.run({ db: form.db_master, sql: form.select_sql_text })
        : await snapMaster.run({ db: form.db_master, table: form.table_master })
    if (!m) return
    const s = await snapSlave.run({ db: form.db_slave, table: form.table_slave })
    if (!s) return
    setMasterCols(m.columns)
    setSlaveCols(s.columns)
    const suggestion = await match.run({
      master_cols: m.columns,
      slave_cols: s.columns,
    })
    setPairs(
      m.columns.map((c, i) => [colName(c), suggestion?.suggestions?.[i] || null]),
    )
    const d = await defaults.run({
      table: form.table_master,
      db_master: form.db_master,
      db_slave: form.db_slave,
    })
    patch({
      period_column: m.period_column,
      slave_period_column: s.period_column,
      line_name: d?.line_name,
      dag_id: d?.dag_id,
    })
    setStep(1)
  }

  const buildSpec = () => ({
    table_master: form.table_master,
    table_slave: form.table_slave,
    db_master: form.db_master,
    db_slave: form.db_slave,
    master_cols: masterCols,
    slave_cols: slaveCols,
    pairs: pairs.filter(([, s]) => s),
    period_column: form.period_column,
    slave_period_column: form.slave_period_column,
    line_name: form.line_name,
    dag_id: form.group_dag_id ? '' : form.dag_id,
    group_dag_id: form.group_dag_id || '',
    mode: form.mode,
    tags: form.tags,
    doc: form.doc,
    select_sql_text: form.source === 'sql' ? form.select_sql_text : '',
  })

  const unmatched = pairs.filter(([, s]) => !s).length

  return (
    <Card title="Новая линия">
      <Steps
        current={step}
        style={{ marginBottom: 16 }}
        onChange={(s) => s <= step && setStep(s)}
        items={[
          { title: 'Откуда и куда' },
          { title: 'Сопоставление колонок' },
          { title: 'Даг и запись' },
        ]}
      />

      {step === 0 && (
        <Form layout="vertical">
          <Row gutter={16}>
            <Col span={6}>
              <Form.Item label="БД ведущей">
                <Select value={form.db_master} onChange={(v) => patch({ db_master: v })} options={DBS} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item label="БД ведомой">
                <Select value={form.db_slave} onChange={(v) => patch({ db_slave: v })} options={DBS} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Источник ведущей">
                <Radio.Group value={form.source} onChange={(e) => patch({ source: e.target.value })}>
                  <Radio.Button value="table">таблица</Radio.Button>
                  <Radio.Button value="sql">свой SELECT</Radio.Button>
                </Radio.Group>
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label={`Ведущая таблица (${form.db_master})`}
                help="со схемой, если нужна: KOKNAEV.IPRKDEPT"
              >
                <TableNameInput
                  value={form.table_master}
                  db={form.db_master}
                  onChange={(v) => patch({ table_master: v })}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label={`Ведомая таблица (${form.db_slave})`}>
                <TableNameInput
                  value={form.table_slave}
                  db={form.db_slave}
                  onChange={(v) => patch({ table_slave: v })}
                />
              </Form.Item>
            </Col>
          </Row>

          {form.source === 'sql' && (
            <Form.Item
              label="SELECT ведущей"
              help="имена колонок берутся из ПСЕВДОНИМОВ запроса — так же, как их видит рантайм"
            >
              <Input.TextArea
                autoSize={{ minRows: 6, maxRows: 20 }}
                style={{ fontFamily: 'monospace' }}
                value={form.select_sql_text}
                onChange={(e) => patch({ select_sql_text: e.target.value })}
              />
            </Form.Item>
          )}

          <Button
            type="primary"
            loading={snapMaster.loading || snapSlave.loading || snapQuery.loading}
            disabled={!form.table_slave || (form.source === 'table' ? !form.table_master : !form.select_sql_text)}
            onClick={snapAll}
          >
            Снять структуры из БД
          </Button>

          <ActionError error={snapMaster.error || snapQuery.error} />
          <ActionError error={snapSlave.error} />
          <ActionError error={match.error} />

          {snapQuery.result?.unknown_types?.length > 0 && (
            <Alert
              style={{ marginTop: 12 }}
              type="warning"
              showIcon
              message="У части колонок тип определить не удалось"
              description={
                'Это константы в запросе (null CHECKDIR, 2 CHECKDIR): ' +
                snapQuery.result.unknown_types.join(', ') +
                '. Тип надо указать руками, иначе сверка структур не сойдётся.'
              }
            />
          )}
        </Form>
      )}

      {step === 1 && (
        <>
          {unmatched > 0 && (
            <Alert
              style={{ marginBottom: 12 }}
              type="warning"
              showIcon
              message={`Без пары осталось колонок ведущей: ${unmatched}`}
              description="Такие колонки в линию не попадут. Если это нарочно — идите дальше."
            />
          )}
          <Table
            size="small"
            bordered
            rowKey={(_, i) => i}
            pagination={false}
            scroll={{ y: 420 }}
            dataSource={pairs.map(([m, s], i) => ({ i, m, s }))}
            columns={[
              { title: '#', dataIndex: 'i', width: 48, render: (v) => v + 1 },
              {
                title: `Ведущая (${form.db_master})`,
                render: (_, r) => {
                  const c = masterCols[r.i]
                  return (
                    <span>
                      <Typography.Text code>{r.m}</Typography.Text>{' '}
                      <Typography.Text type="secondary">{colType(c)}</Typography.Text>{' '}
                      {(c?.is_primary_key || c?.IS_PRIMARY_KEY) && <Tag color="gold">PK</Tag>}
                    </span>
                  )
                },
              },
              {
                title: `Ведомая (${form.db_slave})`,
                render: (_, r) => (
                  <ComboBox
                    style={{ width: '100%' }}
                    value={r.s || ''}
                    options={slaveNames}
                    placeholder="— нет пары —"
                    onChange={(v) =>
                      setPairs((p) => p.map((row, i) => (i === r.i ? [row[0], v || null] : row)))
                    }
                  />
                ),
              },
            ]}
          />
          <Space style={{ marginTop: 12 }}>
            <Button onClick={() => setStep(0)}>Назад</Button>
            <Button type="primary" onClick={() => setStep(2)}>
              Дальше
            </Button>
          </Space>
        </>
      )}

      {step === 2 && (
        <Form layout="vertical">
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="Имя линии (tableNameEtlJobs)">
                <Input value={form.line_name || ''} onChange={(e) => patch({ line_name: e.target.value })} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Режим переноса">
                <Select
                  value={form.mode}
                  onChange={(v) => patch({ mode: v })}
                  options={[
                    'iud',
                    'delete_insert',
                    'section',
                    'section_compare',
                    'section_compare_with_iud',
                    'query_section',
                  ].map((m) => ({ value: m }))}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Колонка периода (ведущая)">
                <ComboBox
                  value={form.period_column}
                  onChange={(v) => patch({ period_column: v })}
                  options={masterCols.map(colName)}
                />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="Колонка периода (ведомая)">
                <ComboBox
                  value={form.slave_period_column}
                  onChange={(v) => patch({ slave_period_column: v })}
                  options={slaveNames}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="dag_id (свой даг)">
                <Input
                  value={form.dag_id || ''}
                  disabled={Boolean(form.group_dag_id)}
                  onChange={(e) => patch({ dag_id: e.target.value })}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Либо положить в составной даг">
                <Input
                  placeholder="MocheckOrclPost"
                  value={form.group_dag_id || ''}
                  onChange={(e) => patch({ group_dag_id: e.target.value })}
                />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="Комментарий линии (_doc)">
            <Input.TextArea
              autoSize={{ minRows: 2, maxRows: 8 }}
              value={form.doc || ''}
              onChange={(e) => patch({ doc: e.target.value })}
            />
          </Form.Item>

          <Space>
            <Button onClick={() => setStep(1)}>Назад</Button>
            <Button
              type="primary"
              loading={preview.loading}
              onClick={() => {
                const s = buildSpec()
                setSpec(s)
                write.reset()
                preview.run(s)
              }}
            >
              Предпросмотр
            </Button>
          </Space>
          <ActionError error={preview.error} onClose={preview.reset} />
        </Form>
      )}

      {preview.result && (
        <div style={{ marginTop: 16 }}>
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
                  overwrite: false,
                })
                .then((res) => res && onCreated?.(spec))
            }
          />
        </div>
      )}
    </Card>
  )
}
