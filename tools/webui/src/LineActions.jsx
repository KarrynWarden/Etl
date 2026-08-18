import { useState } from 'react'
import { Alert, Button, Descriptions, Popconfirm, Space, Typography } from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'

// Действия над линией: операции с ФАЙЛАМИ — архив, восстановление, удаление.
//
// Флагов (`disabled`, `skipAudit`) здесь больше нет, и это принципиально. Они
// живут в конфиге, а значит собираются вместе с ним и попадают на диск ровно
// так же, как любая другая правка: «Настройки» → «Предпросмотр» → «Записать».
// Переключатель, который писал файл сразу по щелчку, был единственным местом,
// где правка уходила на диск незаметно — задел мышью, не увидел, перезагрузил
// страницу, а линия уже отключена в репозитории.
//
// Три РАЗНЫХ способа убрать линию из работы, и путать их дорого:
//   архив     — файл дага уезжает в dags/_archived, Airflow его не парсит.
//               Конфиг и структуры на месте, вернуть можно одной кнопкой;
//   disabled  — флаг в конфиге (вкладка «Настройки»). Даг остаётся, но
//               lineEnabled в рантайме его пропускает. Единственный способ для
//               линии СОСТАВНОГО дага: файл там общий на несколько линий;
//   skipAudit — переносим, но не проверяем аудитом (тоже «Настройки»).
//
// То, что осталось здесь, — перемещение и удаление файлов; собрать их в
// предпросмотр нельзя, поэтому каждое спрашивает подтверждение.
export default function LineActions({ lineKey, spec, onChanged }) {
  const [targets, setTargets] = useState(null)

  const archive = useAction(api.archive)
  const restore = useAction(api.restore)
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

      <Alert
        type="info"
        showIcon
        message="Флаги переехали на вкладку «Настройки»"
        description={
          <>
            «Отключить линию» (disabled) и «Не аудировать» (skipAudit) — это ключи
            конфига, и меняются они как остальные поля: правите на «Настройках»,
            смотрите «Предпросмотр», нажимаете «Записать». Сейчас у линии:{' '}
            <b>{extra.disabled ? 'перенос отключён' : 'перенос включён'}</b>,{' '}
            <b>{extra.skipAudit ? 'аудит выключен' : 'аудит включён'}</b>.
          </>
        }
      />

      <div>
        <Typography.Title level={5}>Убрать даг из видимости Airflow</Typography.Title>
        <Typography.Paragraph type="secondary">
          Архив переносит файл дага в <Typography.Text code>dags/_archived</Typography.Text> —
          этот каталог Airflow не читает. Конфиг и структуры остаются на месте.
          Файл двигается сразу, собрать это в предпросмотр нельзя, поэтому
          спрашиваем подтверждение.
        </Typography.Paragraph>
        <Space wrap>
          <Popconfirm
            title="Убрать даг в архив?"
            description="Файл дага сейчас же переедет в dags/_archived, и Airflow перестанет его видеть."
            okText="В архив"
            cancelText="Нет"
            disabled={inGroup}
            onConfirm={() => archive.run(lineKey).then(after)}
          >
            <Button loading={archive.loading} disabled={inGroup}>
              🗄 В архив
            </Button>
          </Popconfirm>
          <Popconfirm
            title="Вернуть даг из архива?"
            description="Файл сейчас же вернётся в dags/, и Airflow снова начнёт его исполнять."
            okText="Восстановить"
            cancelText="Нет"
            onConfirm={() => restore.run(lineKey).then(after)}
          >
            <Button loading={restore.loading}>♻ Восстановить</Button>
          </Popconfirm>
          {inGroup && (
            <Typography.Text type="secondary">
              линия в составном даге — архивировать нечего, файл общий на несколько
              линий; отключается она флагом disabled на «Настройках»
            </Typography.Text>
          )}
        </Space>
        <ActionError error={archive.error || restore.error} />
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
