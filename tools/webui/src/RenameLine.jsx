import { useState } from 'react'
import { Alert, Button, Col, Input, Popconfirm, Row, Space, Typography } from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import DiffView from './DiffView'

// Переименование линии.
//
// Имя линии (tableNameEtlJobs) — не подпись на форме, а идентификатор, живущий
// в трёх местах сразу, и два из них в базе:
//
//   файлы  ключ фрагмента config.d, значение tableNameEtlJobs в даге, имя
//          файла с DDL триггера;
//   БД     триггер ведущей ПИШЕТ это имя в etl_log_iud_row.tablename, а
//          перенос и аудит ищут по нему же в etl_jobs.tablename. Сравнение
//          дословное.
//
// Поэтому здесь не поле ввода в «Настройках», а отдельная операция с планом:
// поменяй только файлы — и линия замолчит. Триггер продолжит писать старое имя,
// перенос станет искать новое и находить ноль строк. Ошибки не будет ни одной,
// что и делает эту ошибку дорогой.
//
// dag_id меняется здесь же: имя дага и имя линии совпадают не всегда, но
// переименовывают их обычно вместе, а старый файл дага надо снести — иначе
// Airflow увидит два дага сразу.
export default function RenameLine({ lineKey, spec, onChanged }) {
  const [line, setLine] = useState(spec.line_name || '')
  const [dagId, setDagId] = useState(spec.dag_id || '')
  const [plan, setPlan] = useState(null)

  const planning = useAction(api.renamePlan)
  const applying = useAction(api.renameApply)

  const inGroup = Boolean(spec.group_dag_id)
  const changed =
    line.trim() !== (spec.line_name || '') || dagId.trim() !== (spec.dag_id || '')

  const makePlan = () =>
    planning
      .run({ key: lineKey, new_line: line.trim(), new_dag_id: dagId.trim() })
      .then((r) => r && setPlan(r))

  const apply = () =>
    applying
      .run({ key: lineKey, files: plan.files.map((f) => [f.path, f.content]), remove: plan.remove })
      .then((r) => {
        if (!r) return
        setPlan(null)
        onChanged?.(plan.new_key)
      })

  return (
    <div>
      <Typography.Title level={5}>Переименовать</Typography.Title>
      <Typography.Paragraph type="secondary">
        Имя линии — это <Typography.Text code>tableNameEtlJobs</Typography.Text>:
        по нему перенос и аудит ищут группы в <Typography.Text code>etl_jobs</Typography.Text>,
        и его же пишет в журнал триггер ведущей. Сравнение дословное, поэтому
        переименование — не правка поля, а миграция: файлы, база и триггер
        меняются вместе. План покажет всё три части до того, как что-то
        произойдёт.
      </Typography.Paragraph>

      <Row gutter={16}>
        <Col span={12}>
          <Typography.Text type="secondary">Имя линии (tableNameEtlJobs)</Typography.Text>
          <Input
            value={line}
            onChange={(e) => {
              setLine(e.target.value)
              setPlan(null)
            }}
          />
        </Col>
        <Col span={12}>
          <Typography.Text type="secondary">
            dag_id {inGroup && '— линия в составном даге, здесь не меняется'}
          </Typography.Text>
          <Input
            value={dagId}
            disabled={inGroup}
            onChange={(e) => {
              setDagId(e.target.value)
              setPlan(null)
            }}
          />
        </Col>
      </Row>

      <Space style={{ marginTop: 12 }}>
        <Button loading={planning.loading} disabled={!changed} onClick={makePlan}>
          Показать план
        </Button>
        {changed && !plan && (
          <Typography.Text type="secondary">
            ключ станет {line.trim()}
            {spec.db_master}
            {spec.db_slave}
          </Typography.Text>
        )}
      </Space>

      <ActionError error={planning.error} onClose={planning.reset} />
      <ActionError error={applying.error} onClose={applying.reset} />

      {plan && (
        <Space direction="vertical" size="middle" style={{ display: 'flex', marginTop: 16 }}>
          {plan.warnings.map((w, i) => (
            <Alert key={i} type="warning" showIcon message={w} />
          ))}

          <Alert
            type="info"
            showIcon
            message={`Будет записано файлов: ${plan.files.length}`}
            description={
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {plan.files.map((f) => (
                  <li key={f.path}>
                    <Typography.Text code>{f.path}</Typography.Text>
                    {plan.created.includes(f.path) && (
                      <Typography.Text type="success"> — новый</Typography.Text>
                    )}
                  </li>
                ))}
              </ul>
            }
          />

          <Alert
            type="error"
            showIcon
            message={`Будет удалено: ${plan.remove.length}`}
            description={
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {plan.remove.map((p) => (
                  <li key={p}>
                    <Typography.Text code>{p}</Typography.Text>
                  </li>
                ))}
              </ul>
            }
          />

          {/* Разница по дагу — самое читаемое место плана: видно, что
              tableNameEtlJobs поменялся, а имя задачи (do_etl_…) осталось
              прежним, то есть история запусков никуда не делась. */}
          {plan.files
            .filter((f) => f.path.startsWith('dags/') && f.old)
            .map((f) => (
              <div key={f.path}>
                <Typography.Text type="secondary">{f.path}</Typography.Text>
                <DiffView oldText={f.old} newText={f.content} maxHeight={220} />
              </div>
            ))}

          {plan.sql && (
            <div>
              <Typography.Text strong>
                Это надо выполнить в ведущей БД — сразу после записи файлов:
              </Typography.Text>
              <pre
                style={{
                  background: '#fffbe6',
                  border: '1px solid #ffe58f',
                  padding: 8,
                  marginTop: 4,
                  maxHeight: 260,
                  overflow: 'auto',
                  fontSize: 12,
                }}
              >
                {plan.sql}
              </pre>
            </div>
          )}

          <Space>
            <Popconfirm
              title="Переименовать линию?"
              description="Файлы будут записаны, перечисленные — удалены. Не забудьте про SQL и триггер."
              okText="Переименовать"
              okButtonProps={{ danger: true }}
              cancelText="Нет"
              onConfirm={apply}
            >
              <Button danger type="primary" loading={applying.loading}>
                Переименовать в {plan.new_key}
              </Button>
            </Popconfirm>
            <Button onClick={() => setPlan(null)}>Отмена</Button>
          </Space>
        </Space>
      )}
    </div>
  )
}
