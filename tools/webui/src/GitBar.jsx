import { useEffect } from 'react'
import { Alert, Button, Card, Space, Tag, Typography } from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'

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

  const dirty = status.result?.status || []

  return (
    <Card size="small">
      <Space wrap>
        <Typography.Text type="secondary">ветка</Typography.Text>
        <Tag color="geekblue">{status.result?.branch ?? '…'}</Tag>
        {dirty.length ? (
          <Tag color="orange">не запушено файлов: {dirty.length}</Tag>
        ) : (
          <Tag color="green">всё запушено</Tag>
        )}
        <Button
          size="small"
          disabled={!dirty.length}
          loading={push.loading}
          onClick={() => push.run({ message: 'конструктор: правка линий' })}
        >
          Закоммитить и запушить
        </Button>
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
