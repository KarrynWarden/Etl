import { useEffect, useState } from 'react'
import { Alert, Button, Card, Checkbox, Collapse, Space, Tag, Typography } from 'antd'

import ActionError from './ActionError'

// Предпросмотр собранных файлов.
//
// Ключевая цифра здесь — сколько файлов реально изменится. Конструктор
// пересобирает ВСЕ файлы линии, но пишет только отличающиеся (skip_unchanged),
// и в режиме правки это важно видеть до записи: правили период — меняется
// конфиг, а структуры и даг остаются как были. В старом интерфейсе это был
// список путей в общем потоке вывода, где «изменится» и «не изменится»
// выглядели одинаково.
//
// Галочка у файла — не украшение. Часть дагов правлена руками, и пересборка
// шаблона может, например, поменять task_id: для Airflow это НОВАЯ задача,
// история прежней остаётся висеть отдельно. Поэтому решение «переписывать ли
// этот конкретный файл» оставлено человеку — снятая галочка означает, что
// файл на диске остаётся как есть.
export default function FilesPreview({
  files,
  unchanged,
  created,
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
  const createdSet = new Set(created || [])
  const changed = (files || []).filter((f) => !unchangedSet.has(f.path))
  const [skipped, setSkipped] = useState(() => new Set())

  // Новый предпросмотр — новый набор файлов: отметки от прошлого не переносим,
  // иначе снятая когда-то галочка молча пропустила бы файл в следующий раз.
  useEffect(() => setSkipped(new Set()), [files])

  const toggle = (path, on) =>
    setSkipped((prev) => {
      const next = new Set(prev)
      if (on) next.delete(path)
      else next.add(path)
      return next
    })

  const selected = changed.filter((f) => !skipped.has(f.path))

  return (
    <Card
      title={
        <Space wrap>
          Предпросмотр
          <Tag color={changed.length ? 'orange' : 'default'}>
            изменится файлов: {changed.length - createdSet.size}
          </Tag>
          {createdSet.size > 0 && <Tag color="green">будет создано: {createdSet.size}</Tag>}
          <Tag>без изменений: {unchangedSet.size}</Tag>
          {skipped.size > 0 && <Tag color="red">пропустить: {skipped.size}</Tag>}
        </Space>
      }
      extra={
        <Space>
          <Button
            type="primary"
            danger={selected.length > 0}
            disabled={!selected.length}
            loading={busy}
            onClick={() => onWrite(selected)}
          >
            Записать {selected.length < changed.length ? `выбранное (${selected.length})` : 'на диск'}
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

      {changed.length > 1 && (
        <Space style={{ marginBottom: 8 }}>
          <Button size="small" onClick={() => setSkipped(new Set())}>
            Отметить все
          </Button>
          <Button
            size="small"
            onClick={() => setSkipped(new Set(changed.map((f) => f.path)))}
          >
            Снять все
          </Button>
          <Typography.Text type="secondary">
            снятая галочка — файл на диске остаётся как есть
          </Typography.Text>
        </Space>
      )}

      <Collapse
        style={{ marginTop: changed.length ? 0 : 12 }}
        items={(files || []).map((f) => {
          const isChanged = !unchangedSet.has(f.path)
          return {
            key: f.path,
            label: (
              <Space onClick={(e) => e.stopPropagation()}>
                {isChanged && (
                  <Checkbox
                    checked={!skipped.has(f.path)}
                    onChange={(e) => toggle(f.path, e.target.checked)}
                  />
                )}
                <Typography.Text code>{f.path}</Typography.Text>
                {!isChanged ? (
                  <Tag>без изменений</Tag>
                ) : skipped.has(f.path) ? (
                  <Tag color="red">не писать</Tag>
                ) : createdSet.has(f.path) ? (
                  <Tag color="green">будет создан</Tag>
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
          }
        })}
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
