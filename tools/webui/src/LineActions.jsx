import { useState } from 'react'
import { Alert, Button, Descriptions, Popconfirm, Space, Switch, Typography } from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'

// Действия над линией: архив, флаги, удаление.
//
// Три РАЗНЫХ способа убрать линию из работы, и путать их дорого:
//   архив     — файл дага уезжает в dags/_archived, Airflow его не парсит.
//               Конфиг и структуры на месте, вернуть можно одной кнопкой;
//   disabled  — флаг в конфиге. Даг остаётся, но lineEnabled в рантайме его
//               пропускает. Единственный способ для линии СОСТАВНОГО дага:
//               файл там общий на несколько линий, прятать его нельзя;
//   skipAudit — переносим, но не проверяем аудитом.
// Удаление — насовсем, и оно единственное необратимо, поэтому сначала
// показывает полный список того, что снесёт.
export default function LineActions({ lineKey, spec, onChanged }) {
  const [targets, setTargets] = useState(null)

  const archive = useAction(api.archive)
  const restore = useAction(api.restore)
  const setFlag = useAction((flag, value) => api.setFlag({ key: lineKey, flag, value }))
  const deleteTargets = useAction(api.deleteTargets)
  const remove = useAction(api.deleteLine)

  const extra = spec.extra || {}
  const inGroup = Boolean(spec.group_dag_id)
  const after = (res) => {
    if (res) onChanged?.()
    return res
  }

  return (
    <Space direction="vertical" size="large" style={{ display: 'flex' }}>
      <Descriptions size="small" column={1} bordered title="Состояние">
        <Descriptions.Item label="Ключ конфига">{lineKey}</Descriptions.Item>
        <Descriptions.Item label="Даг">
          {spec.group_dag_id ? `составной: ${spec.group_dag_id}` : spec.dag_id || '—'}
        </Descriptions.Item>
      </Descriptions>

      <div>
        <Typography.Title level={5}>Убрать из работы</Typography.Title>
        <Space direction="vertical" style={{ display: 'flex' }}>
          <Space>
            <Switch
              checked={Boolean(extra.skipAudit)}
              loading={setFlag.loading}
              onChange={(v) => setFlag.run('skipAudit', v).then(after)}
            />
            <span>Не аудировать линию (skipAudit) — переносим, но не проверяем</span>
          </Space>

          <Space>
            <Button
              loading={archive.loading}
              disabled={inGroup}
              onClick={() => archive.run(lineKey).then(after)}
            >
              🗄 В архив
            </Button>
            <Button loading={restore.loading} onClick={() => restore.run(lineKey).then(after)}>
              ♻ Восстановить
            </Button>
            {inGroup && (
              <Typography.Text type="secondary">
                линия в составном даге — архивировать нечего, файл общий; используйте флаг ниже
              </Typography.Text>
            )}
          </Space>

          <Space>
            <Switch
              checked={Boolean(spec.disabled)}
              loading={setFlag.loading}
              onChange={(v) => setFlag.run('disabled', v).then(after)}
            />
            <span>Отключить линию (disabled) — даг остаётся, но пропускает её</span>
          </Space>
        </Space>
        <ActionError error={archive.error || restore.error || setFlag.error} />
        {(archive.result || restore.result) && (
          <Alert
            style={{ marginTop: 8 }}
            type="success"
            showIcon
            message={String((archive.result || restore.result).done)}
          />
        )}
      </div>

      <div>
        <Typography.Title level={5} type="danger">
          Удалить насовсем
        </Typography.Title>
        <Typography.Paragraph type="secondary">
          Уберёт фрагмент конфига, файл дага и структуры, если они не нужны другим линиям.
          Отменить это нельзя — только восстановить из git.
        </Typography.Paragraph>
        <Space>
          <Button
            loading={deleteTargets.loading}
            onClick={() => deleteTargets.run(lineKey).then((r) => r && setTargets(r.targets))}
          >
            Показать, что будет удалено
          </Button>
          <Popconfirm
            title="Удалить линию насовсем?"
            description="Файлы из списка будут удалены с диска."
            okText="⚠️ Да, удалить"
            okButtonProps={{ danger: true }}
            cancelText="Нет"
            disabled={!targets}
            onConfirm={() => remove.run(lineKey).then(after)}
          >
            <Button danger disabled={!targets} loading={remove.loading}>
              Удалить
            </Button>
          </Popconfirm>
        </Space>
        <ActionError error={deleteTargets.error || remove.error} />
        {targets && (
          <Alert
            style={{ marginTop: 8 }}
            type="warning"
            showIcon
            message={`Будет удалено файлов: ${targets.length}`}
            description={
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {targets.map((t) => (
                  <li key={String(t)}>
                    <Typography.Text code>{String(t)}</Typography.Text>
                  </li>
                ))}
              </ul>
            }
          />
        )}
        {remove.result && (
          <Alert style={{ marginTop: 8 }} type="success" showIcon message="Линия удалена" />
        )}
      </div>
    </Space>
  )
}
