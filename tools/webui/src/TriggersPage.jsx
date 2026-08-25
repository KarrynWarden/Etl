import { useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Modal,
  Space,
  Table,
  Tag,
  Typography,
} from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'

// Триггеры линий: что нужно, что реально стоит в БД, и готовый DDL.
//
// Конструктор НЕ выполняет DDL в боевой БД — это делает администратор, чтобы
// правки шли через одни руки. Поэтому здесь три вещи: показать, кому триггер
// нужен; сверить с тем, что стоит; отдать текст, который можно передать DBA.
// Статусы сверки приходят из tools/trigger_builder.check_targets. Английское
// слово в ячейке ничего не объясняет — расшифровываем.
const STATUS_COLOR = {
  ok: 'green',
  warn: 'orange',
  error: 'red',
  missing: 'red',
  skip: 'default',
  fail: 'red',
}
const STATUS_TEXT = {
  ok: 'стоит и совпадает',
  warn: 'стоит, но с оговорками',
  error: 'стоит неправильно',
  missing: 'триггера нет',
  skip: 'не проверялся',
  fail: 'проверить не удалось',
}

// Порядок строк после сверки. Список — это список ДЕЛ: сначала то, что чинить,
// потом всё остальное. Сортировка по имени линии оставляла «триггера нет»
// вперемешку с «всё совпало», и разобрать, что именно требует внимания,
// получалось только вычитыванием таблицы целиком.
const STATUS_ORDER = {
  error: 0,      // стоит, но не тот — самое опасное: молчит и врёт
  missing: 1,    // триггера нет — перенос не увидит изменений
  fail: 2,       // проверить не удалось — неизвестность хуже «нет»
  warn: 3,
  skip: 4,
  ok: 5,
}

export default function TriggersPage() {
  const [onlyNeeded, setOnlyNeeded] = useState(true)
  const [hideOk, setHideOk] = useState(false)
  const [ddl, setDdl] = useState(null)

  const targets = useAction(api.triggerTargets)
  const check = useAction(api.triggerCheck)
  const build = useAction(api.triggerBuild)

  useEffect(() => {
    targets.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const checked = check.result?.results || {}
  const checkedAny = Object.keys(checked).length > 0
  const statusOf = (key) => checked[key]?.status || (checked[key] ? 'ok' : null)

  const rows = (targets.result?.targets || [])
    .filter((t) => !onlyNeeded || t.needs)
    .filter((t) => !hideOk || statusOf(t.key) !== 'ok')
    .slice()
    .sort((a, b) => {
      // до сверки порядок не меняем — сортировать нечего, и перестановка
      // строк на пустом месте только сбивает
      if (!checkedAny) return 0
      const sa = STATUS_ORDER[statusOf(a.key)] ?? 9
      const sb = STATUS_ORDER[statusOf(b.key)] ?? 9
      return sa - sb || String(a.key).localeCompare(String(b.key))
    })
  const okCount = Object.values(checked).filter((r) => (r.status || 'ok') === 'ok').length

  const columns = [
    { title: 'Линия', dataIndex: 'key', render: (v, r) => (
        <span>
          <b>{v}</b>
          <br />
          <Typography.Text type="secondary">{r.table}</Typography.Text>
        </span>
      ) },
    { title: 'Режим', dataIndex: 'mode', width: 200 },
    {
      title: 'Нужен',
      dataIndex: 'needs',
      width: 90,
      render: (v) => (v ? <Tag color="orange">да</Tag> : <Tag>нет</Tag>),
    },
    {
      title: 'Колонки ведущей',
      render: (_, r) => (
        <span>
          период: <Typography.Text code>{String(r.period_column ?? '—')}</Typography.Text>
          <br />
          ключ: <Typography.Text code>{(r.pk_columns || []).join(', ') || '—'}</Typography.Text>
          {(r.also || []).length > 0 && (
            <>
              <br />
              {/* Триггер один на таблицу, а меток он пишет столько, сколько
                  линий и зависимостей на ней висит. Без этой строки DDL
                  выглядит так, будто в него попало лишнее. */}
              <Typography.Text type="secondary">
                тот же триггер пишет также:{' '}
                {(r.also || []).map((n) => (
                  <Typography.Text code key={n}>{n}</Typography.Text>
                ))}
              </Typography.Text>
            </>
          )}
          {r.note && (
            <>
              <br />
              <Typography.Text type="warning">{r.note}</Typography.Text>
            </>
          )}
        </span>
      ),
    },
    {
      title: 'В БД',
      width: 260,
      render: (_, r) => {
        const res = checked[r.key]
        if (!res) return <Typography.Text type="secondary">не сверялось</Typography.Text>
        const status = res.status || 'ok'
        const trouble = [...(res.problems || []), ...(res.notes || [])]
        return (
          <Space direction="vertical" size={2}>
            <Tag color={STATUS_COLOR[status] || 'orange'}>
              {STATUS_TEXT[status] || String(status)}
            </Tag>
            {trouble.map((p, i) => (
              <Typography.Text key={i} type={res.problems?.includes(p) ? 'danger' : 'secondary'}
                               style={{ fontSize: 12 }}>
                {String(p)}
              </Typography.Text>
            ))}
          </Space>
        )
      },
    },
    {
      title: '',
      width: 130,
      render: (_, r) => (
        <Button
          size="small"
          disabled={!r.needs || !r.period_column || !(r.pk_columns || []).length}
          loading={build.loading}
          onClick={() =>
            build
              .run({
                db: r.db,
                table: r.table,
                tablename: r.tablename,
                period_column: r.period_column,
                pk_columns: r.pk_columns,
              })
              .then((res) => res && setDdl({ ...res, key: r.key }))
          }
        >
          Показать DDL
        </Button>
      ),
    },
  ]

  return (
    <Card
      title="Триггеры линий"
      extra={
        <Space>
          <Checkbox checked={onlyNeeded} onChange={(e) => setOnlyNeeded(e.target.checked)}>
            только те, кому триггер нужен
          </Checkbox>
          <Checkbox
            checked={hideOk}
            disabled={!checkedAny}
            onChange={(e) => setHideOk(e.target.checked)}
          >
            скрыть совпавшие{checkedAny ? ` (${okCount})` : ''}
          </Checkbox>
          <Button
            loading={check.loading}
            disabled={!rows.length}
            // Проверяем ВСЕ подходящие линии, а не только видимые: иначе
            // «скрыть совпавшие» после первой же сверки сузило бы вторую до
            // проблемных, и починенное перестало бы перепроверяться.
            onClick={() =>
              check.run(
                (targets.result?.targets || [])
                  .filter((t) => !onlyNeeded || t.needs)
                  .map((t) => t.key),
              )
            }
          >
            Проверить в БД
          </Button>
          <Button loading={targets.loading} onClick={() => targets.run()}>
            Обновить
          </Button>
        </Space>
      }
    >
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message="DDL в боевой БД конструктор не выполняет"
        description="Текст триггера здесь можно посмотреть и передать администратору БД — так все правки боевых объектов идут через одни руки."
      />

      <ActionError error={targets.error} />
      <ActionError error={check.error} onClose={check.reset} />
      <ActionError error={build.error} onClose={build.reset} />

      {/* Отчёт по БД, а не по линиям: самая частая причина «все линии красные»
          — это не триггеры, а «подключиться не удалось». Без этого блока
          сверка выглядела бы как приговор всем линиям сразу. */}
      {Object.entries(check.result?.db_reports || {}).map(([db, rep]) => (
        <Alert
          key={db}
          style={{ marginBottom: 12 }}
          type={rep.status === 'ok' ? 'success' : rep.status === 'fail' ? 'error' : 'warning'}
          showIcon
          message={`${db}: реквизиты ${rep.cred}, журнал ${rep.journal}`}
          description={
            (rep.messages || []).length ? (
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {rep.messages.map((m, i) => (
                  <li key={i}>{String(m)}</li>
                ))}
              </ul>
            ) : (
              'служебные объекты на месте'
            )
          }
        />
      ))}

      <Table
        rowKey="key"
        size="small"
        bordered
        loading={targets.loading}
        dataSource={rows}
        columns={columns}
        pagination={false}
        scroll={{ y: '60vh' }}
      />

      <Modal
        open={Boolean(ddl)}
        onCancel={() => setDdl(null)}
        footer={<Button onClick={() => setDdl(null)}>Закрыть</Button>}
        width={900}
        title={ddl ? `DDL триггера: ${ddl.name}` : ''}
      >
        {ddl && (
          <>
            <Typography.Paragraph type="secondary">
              журнал: <Typography.Text code>{ddl.journal}</Typography.Text>
            </Typography.Paragraph>
            <pre style={{ maxHeight: '55vh', overflow: 'auto' }}>{ddl.text}</pre>
          </>
        )}
      </Modal>
    </Card>
  )
}
