import { useEffect, useMemo, useState } from 'react'
import {
  Button,
  Card,
  Col,
  Descriptions,
  Divider,
  Form,
  Input,
  Row,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import ComboBox from './ComboBox'
import FilesPreview from './FilesPreview'

const MODES = [
  'iud',
  'delete_insert',
  'section',
  'section_compare',
  'section_compare_with_iud',
  'query_section',
]

// Правка существующей линии: подгрузить спецификацию, поменять поля,
// посмотреть, ЧТО именно изменится, и только потом записать.
//
// Предпросмотр отделён от записи намеренно — на сервере это разные маршруты
// (preview ничего не пишет на диск). Здесь это даёт главное свойство режима
// правки: видно, что меняется РОВНО то, что правили, а остальные файлы
// помечены «без изменений».
export default function LineForm({ lineKey, onChanged }) {
  const [spec, setSpec] = useState(null)
  const [placement, setPlacement] = useState(null)

  const load = useAction(api.line)
  const preview = useAction(api.preview)
  const write = useAction(api.write)
  const tagsQuery = useAction(api.tags)

  useEffect(() => {
    if (!lineKey) return
    preview.reset()
    write.reset()
    load.run(lineKey).then((data) => {
      if (!data) return
      setSpec(data.spec)
      setPlacement(data.placement)
    })
    tagsQuery.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lineKey])

  const columnNames = useMemo(() => {
    const cols = spec?.master_cols || []
    return cols.map((c) => c.column_name || c.COLUMN_NAME).filter(Boolean)
  }, [spec])

  if (!lineKey) {
    return (
      <Card>
        <Typography.Text type="secondary">
          Выберите линию слева — или создайте новую.
        </Typography.Text>
      </Card>
    )
  }

  if (load.loading) {
    return (
      <Card>
        <Spin /> <Typography.Text type="secondary">Читаю линию…</Typography.Text>
      </Card>
    )
  }

  if (load.error) {
    return (
      <Card title={lineKey}>
        <ActionError error={load.error} />
      </Card>
    )
  }

  if (!spec) return null

  const patch = (changes) => setSpec((prev) => ({ ...prev, ...changes }))

  return (
    <Space direction="vertical" size="middle" style={{ display: 'flex' }}>
      <Card title={<Space>{lineKey}{placement?.group_dag ? <Tag color="blue">составной даг: {placement.group_dag}</Tag> : <Tag>свой даг</Tag>}</Space>}>
        <Descriptions size="small" column={2} bordered>
          <Descriptions.Item label="Ведущая">{spec.table_master}</Descriptions.Item>
          <Descriptions.Item label="Ведомая">{spec.table_slave}</Descriptions.Item>
          <Descriptions.Item label="Направление">
            {spec.db_master} → {spec.db_slave}
          </Descriptions.Item>
          <Descriptions.Item label="Колонок сопоставлено">
            {(spec.pairs || []).length}
          </Descriptions.Item>
        </Descriptions>

        <Divider orientation="left" plain>
          Что можно поменять
        </Divider>

        <Form layout="vertical">
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="Режим переноса">
                <Select
                  value={spec.mode}
                  onChange={(v) => patch({ mode: v })}
                  options={MODES.map((m) => ({ value: m, label: m }))}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item
                label="Колонка периода (ведущая)"
                help="Список — колонки ведущей; можно вписать своё"
              >
                <ComboBox
                  value={spec.period_column}
                  onChange={(v) => patch({ period_column: v })}
                  options={columnNames}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Колонка периода (ведомая)">
                <ComboBox
                  value={spec.slave_period_column}
                  onChange={(v) => patch({ slave_period_column: v })}
                  options={(spec.slave_cols || []).map(
                    (c) => c.column_name || c.COLUMN_NAME,
                  )}
                />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item label="Теги дага" help="Свои значения вводятся как есть">
                <Select
                  mode="tags"
                  value={spec.tags || []}
                  onChange={(v) => patch({ tags: v })}
                  options={(tagsQuery.result?.tags || []).map((t) => ({ value: t }))}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Комментарий линии (_doc)">
                <Input.TextArea
                  autoSize={{ minRows: 2, maxRows: 6 }}
                  value={spec.doc || ''}
                  onChange={(e) => patch({ doc: e.target.value })}
                />
              </Form.Item>
            </Col>
          </Row>
        </Form>

        <Space>
          <Button
            type="primary"
            loading={preview.loading}
            onClick={() => {
              write.reset()
              preview.run(spec)
            }}
          >
            Предпросмотр
          </Button>
          <Typography.Text type="secondary">
            ничего не записывает — показывает, какие файлы изменятся
          </Typography.Text>
        </Space>

        <ActionError error={preview.error} onClose={preview.reset} />
      </Card>

      {preview.result && (
        <FilesPreview
          files={preview.result.files}
          unchanged={preview.result.unchanged}
          busy={write.loading}
          error={write.error}
          onWrite={() =>
            write
              .run({
                files: preview.result.files.map((f) => [f.path, f.content]),
                overwrite: true,
              })
              .then((res) => res && onChanged?.(res))
          }
          written={write.result}
        />
      )}
    </Space>
  )
}
