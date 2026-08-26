import { useEffect } from 'react'
import { Alert, Button, Card, Popconfirm, Space, Tag, Typography } from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import DeployStatus from './DeployStatus'

// Состояние рабочего клона. Вынесено наверх намеренно: конструктор пишет файлы
// в репозиторий, и «сохранил, но не запушил» — самая частая причина того, что
// на сервере ничего не изменилось.
export default function GitBar({ reloadToken }) {
  const status = useAction(api.gitStatus)
  const push = useAction(api.gitPush)

  useEffect(() => {
    status.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reloadToken, push.result])

  // Сервис отдаёт список путей. Массив проверяем всё равно: одна страница на
  // весь конструктор, и падение здесь уносит ВСЁ — вкладки, формы,
  // незаписанные правки. Ошибку формы ответа лучше показать строкой, чем
  // погасить экран.
  const raw = status.result?.status
  const dirty = Array.isArray(raw)
    ? raw
    : typeof raw === 'string'
      ? raw.split('\n').filter(Boolean)
      : []

  return (
    <Card size="small">
      <Space wrap>
        {/* Перезапуск сервисов — рядом с состоянием клона: и то и другое
            отвечает на «уехало ли сделанное на сервер». */}
        <DeployStatus reloadToken={reloadToken} />
        <Typography.Text type="secondary">ветка</Typography.Text>
        <Tag color="geekblue">{status.result?.branch ?? '…'}</Tag>
        {dirty.length ? (
          <Tag color="orange">не запушено файлов: {dirty.length}</Tag>
        ) : (
          <Tag color="green">всё запушено</Tag>
        )}
        {/* Пуш уходит наружу и забирает ВСЁ незакоммиченное в клоне, не только
            то, что правил ты. Поэтому спрашиваем и показываем список файлов в
            самом вопросе. */}
        <Popconfirm
          title="Закоммитить и запушить?"
          description={
            <div style={{ maxWidth: 460 }}>
              В коммит уйдут все перечисленные файлы — и те, что правил не ты,
              если в клоне осталось чужое:
              <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
                {dirty.map((f) => (
                  <li key={f}>
                    <Typography.Text code>{f}</Typography.Text>
                  </li>
                ))}
              </ul>
            </div>
          }
          okText="Запушить"
          cancelText="Нет"
          disabled={!dirty.length}
          onConfirm={() => push.run({ message: 'конструктор: правка линий' })}
        >
          <Button size="small" disabled={!dirty.length} loading={push.loading}>
            Закоммитить и запушить
          </Button>
        </Popconfirm>
      </Space>

      {dirty.length > 0 && (
        <Alert
          style={{ marginTop: 8 }}
          type="info"
          message={dirty.join('  ·  ')}
        />
      )}

      <ActionError error={status.error} />
      <ActionError error={push.error} onClose={push.reset} />
    </Card>
  )
}
