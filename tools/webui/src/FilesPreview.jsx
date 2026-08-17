import { Alert, Button, Card, Collapse, Space, Tag, Typography } from 'antd'

import ActionError from './ActionError'

// Предпросмотр собранных файлов.
//
// Ключевая цифра здесь — сколько файлов реально изменится. Конструктор
// пересобирает ВСЕ файлы линии, но пишет только отличающиеся (skip_unchanged),
// и в режиме правки это важно видеть до записи: правили период — меняется
// конфиг, а структуры и даг остаются как были. В старом интерфейсе это был
// список путей в общем потоке вывода, где «изменится» и «не изменится»
// выглядели одинаково.
export default function FilesPreview({
  files,
  unchanged,
  onWrite,
  busy,
  error,
  written,
  onPush,
  pushing,
  pushError,
  pushed,
}) {
  const unchangedSet = new Set(unchanged || [])
  const changed = (files || []).filter((f) => !unchangedSet.has(f.path))

  return (
    <Card
      title={
        <Space>
          Предпросмотр
          <Tag color={changed.length ? 'orange' : 'default'}>
            изменится файлов: {changed.length}
          </Tag>
          <Tag>без изменений: {unchangedSet.size}</Tag>
        </Space>
      }
      extra={
        <Space>
          <Button
            type="primary"
            danger={changed.length > 0}
            disabled={!changed.length}
            loading={busy}
            onClick={onWrite}
          >
            Записать на диск
          </Button>
          {onPush && (
            <Button disabled={!written} loading={pushing} onClick={onPush}>
              Записать и запушить
            </Button>
          )}
        </Space>
      }
    >
      {!changed.length && (
        <Alert
          type="success"
          showIcon
          message="Менять нечего"
          description="Собранные файлы совпадают с тем, что уже лежит на диске."
        />
      )}

      <Collapse
        style={{ marginTop: changed.length ? 0 : 12 }}
        items={(files || []).map((f) => ({
          key: f.path,
          label: (
            <Space>
              <Typography.Text code>{f.path}</Typography.Text>
              {unchangedSet.has(f.path) ? (
                <Tag>без изменений</Tag>
              ) : (
                <Tag color="orange">изменится</Tag>
              )}
            </Space>
          ),
          children: (
            <Typography.Paragraph>
              <pre style={{ maxHeight: 360, overflow: 'auto', margin: 0 }}>
                {f.content}
              </pre>
            </Typography.Paragraph>
          ),
        }))}
      />

      <ActionError error={error} />
      <ActionError error={pushError} />
      {pushed && (
        <Alert
          style={{ marginTop: 12 }}
          type="success"
          showIcon
          message="Запушено в git"
          description={String(pushed.done)}
        />
      )}

      {written && (
        <Alert
          style={{ marginTop: 12 }}
          type="success"
          showIcon
          message={`Записано файлов: ${written.written.length}`}
          description={
            written.written.length ? (
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {written.written.map((p) => (
                  <li key={p}>
                    <Typography.Text code>{p}</Typography.Text>
                  </li>
                ))}
              </ul>
            ) : (
              'Все файлы уже были такими — на диск ничего не ушло.'
            )
          }
        />
      )}
    </Card>
  )
}
