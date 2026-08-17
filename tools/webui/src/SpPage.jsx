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
  Segmented,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tabs,
  Tag,
  Typography,
} from 'antd'
import { UndoOutlined } from '@ant-design/icons'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import ComboBox from './ComboBox'
import FilesPreview from './FilesPreview'

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

export default function SpPage() {
  const [kind, setKind] = useState('regular')
  const [selected, setSelected] = useState(null)
  const [filter, setFilter] = useState('')

  const lines = useAction(api.spLines)

  useEffect(() => {
    lines.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const shown = useMemo(() => {
    const all = lines.result?.lines || []
    const needle = filter.trim().toLowerCase()
    return all.filter(
      (l) => l.kind === kind && (!needle || l.key.toLowerCase().includes(needle)),
    )
  }, [lines.result, kind, filter])

  return (
    <Row gutter={16}>
      <Col xs={24} md={8} lg={6}>
        <Card
          size="small"
          title="Линии"
          extra={
            <Button size="small" loading={lines.loading} onClick={() => lines.run()}>
              Обновить
            </Button>
          }
        >
          <Segmented
            block
            value={kind}
            onChange={(v) => {
              setKind(v)
              setSelected(null)
            }}
            options={Object.entries(KINDS).map(([value, label]) => ({ value, label }))}
          />
          <Input.Search
            allowClear
            style={{ margin: '8px 0' }}
            placeholder="фильтр по имени"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          <Typography.Text type="secondary">
            показано {shown.length} из {(lines.result?.lines || []).filter((l) => l.kind === kind).length}
          </Typography.Text>

          <ActionError error={lines.error} />

          <div style={{ maxHeight: '55vh', overflow: 'auto', marginTop: 8 }}>
            {(lines.loading && !lines.result ? [] : shown).map((l) => (
              <div
                key={l.key}
                onClick={() => setSelected(l)}
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
        <SpForm entry={selected} onChanged={() => lines.run()} />
      </Col>
    </Row>
  )
}

function SpForm({ entry, onChanged }) {
  const [spec, setSpec] = useState(null)
  const [original, setOriginal] = useState(null)
  const [targets, setTargets] = useState(null)
  const [ddl, setDdl] = useState(null)

  const load = useAction(api.spLine)
  const preview = useAction(api.spPreview)
  const write = useAction(api.write)
  const setDisabled = useAction(api.spSetDisabled)
  const move = useAction(api.spMove)
  const delTargets = useAction(api.spDeleteTargets)
  const remove = useAction(api.spDelete)
  const auditTrigger = useAction(api.spAuditTrigger)

  useEffect(() => {
    if (!entry) return
    preview.reset()
    write.reset()
    setTargets(null)
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

  if (load.loading && !spec)
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
  const slaveNames = (spec.slave_cols || []).map((c) =>
    typeof c === 'string' ? c : c?.column_name || c?.COLUMN_NAME || '',
  )

  const settings = (
    <Form layout="vertical">
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

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="Ведущая таблица">
            <Input
              value={spec.master_table || ''}
              onChange={(e) => patch({ master_table: e.target.value })}
            />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="Ведомая таблица">
            <Input
              value={spec.slave_table || ''}
              onChange={(e) => patch({ slave_table: e.target.value })}
            />
          </Form.Item>
        </Col>
      </Row>

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
          <Form.Item
            label="Источник ведущей"
            help={
              spec.src_mode === 'all'
                ? 'своего Select.sql нет — рантайм сам берёт SELECT * FROM ведущей'
                : spec.src_mode === 'table'
                  ? 'Select.sql собирается из сопоставленных колонок'
                  : 'Select.sql — ваш запрос, сохраняется как есть'
            }
          >
            <Radio.Group
              value={spec.src_mode}
              onChange={(e) => patch({ src_mode: e.target.value })}
            >
              <Radio.Button value="all">вся таблица</Radio.Button>
              <Radio.Button value="table">выбранные колонки</Radio.Button>
              <Radio.Button value="custom">своё SELECT</Radio.Button>
            </Radio.Group>
          </Form.Item>
        </Col>
      </Row>

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
      {spec.src_mode === 'all' && (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="Источник — вся таблица, имён колонок ведущей здесь нет"
          description="Рантайм подставляет SELECT * FROM ведущей, поэтому колонки сопоставляются по ПОЗИЦИИ: i-я колонка выборки ложится в i-ю колонку INSERT."
        />
      )}
      {spec.src_mode === 'custom' && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="Для своего SELECT сопоставлены должны быть ВСЕ колонки"
          description="Вставка идёт по порядку колонок запроса, пропуски недопустимы."
        />
      )}
      <Table
        size="small"
        bordered
        pagination={false}
        scroll={{ y: 380 }}
        rowKey={(_, i) => i}
        dataSource={(spec.pairs || []).map(([m, s], i) => ({ i, m, s }))}
        columns={[
          { title: '#', dataIndex: 'i', width: 48, render: (v) => v + 1 },
          {
            title: `Ведущая (${spec.db_master})`,
            render: (_, r) => <Typography.Text code>{r.m}</Typography.Text>,
          },
          {
            title: `Ведомая (${spec.db_slave})`,
            render: (_, r) => (
              <ComboBox
                style={{ width: '100%' }}
                value={r.s || ''}
                options={slaveNames}
                placeholder="— нет пары —"
                onChange={(v) =>
                  patch({
                    pairs: spec.pairs.map((row, i) => (i === r.i ? [row[0], v || null] : row)),
                  })
                }
              />
            ),
          },
        ]}
      />
    </>
  )

  const sql = (
    <Form layout="vertical">
      <Form.Item
        label={`Select.sql${spec.select_sql ? ` — ${spec.select_sql}` : ''}`}
        help={
          spec.src_mode === 'custom'
            ? 'свой запрос — сохраняется как есть'
            : 'собирается из ведущей таблицы и сопоставленных колонок'
        }
      >
        <Input.TextArea
          autoSize={{ minRows: 5, maxRows: 20 }}
          style={{ fontFamily: 'monospace' }}
          value={spec.select_sql_text || ''}
          disabled={spec.src_mode !== 'custom'}
          onChange={(e) => patch({ select_sql_text: e.target.value })}
        />
      </Form.Item>
      <Form.Item
        label={`Add.sql${spec.add_sql ? ` — ${spec.add_sql}` : ''}`}
        help="собирается автоматически из ведомой таблицы и сопоставления"
      >
        <Input.TextArea
          autoSize={{ minRows: 3, maxRows: 12 }}
          style={{ fontFamily: 'monospace' }}
          value={spec.add_sql_text || ''}
          disabled
        />
      </Form.Item>
    </Form>
  )

  const actions = (
    <Space direction="vertical" size="large" style={{ display: 'flex' }}>
      <div>
        <Typography.Title level={5}>Участие в работе</Typography.Title>
        <Space>
          <Switch
            checked={!entry.disabled}
            loading={setDisabled.loading}
            onChange={(v) =>
              setDisabled
                .run({ kind: entry.kind, key: entry.key, value: !v })
                .then((r) => r && onChanged?.())
            }
          />
          <span>{entry.disabled ? 'отключена — даг её пропускает' : 'включена'}</span>
        </Space>
        <ActionError error={setDisabled.error} />
      </div>

      <div>
        <Typography.Title level={5}>Перевести в другой тип</Typography.Title>
        <Typography.Paragraph type="secondary">
          {entry.kind === 'regular'
            ? 'Сейчас это регулярный справочник: обновляется по заданию (isokaudit = 0). Перевод в разовый снимет с него это ожидание.'
            : 'Сейчас это разовый перенос: запускается руками. Перевод в регулярный подключит его к дагу справочников.'}
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
            <b>{entry.key}</b>
            <Tag>{KINDS[entry.kind]}</Tag>
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
            { key: 'act', label: 'Действия', children: actions },
          ]}
        />
        <ActionError error={preview.error} onClose={preview.reset} />
      </Card>

      {preview.result && (
        <FilesPreview
          files={preview.result.files}
          unchanged={preview.result.unchanged}
          busy={write.loading}
          error={write.error}
          written={write.result}
          onWrite={() =>
            write
              .run({
                files: preview.result.files.map((f) => [f.path, f.content]),
                overwrite: true,
              })
              .then((r) => {
                if (!r) return
                setOriginal(JSON.stringify(spec))
                onChanged?.()
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
