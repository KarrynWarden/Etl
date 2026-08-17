import { useEffect, useMemo, useState } from 'react'
import { Badge, Card, Input, List, Segmented, Spin, Typography } from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'

// Список линий. Архивные показываются отдельно, а не вперемешку: линия в
// архиве всё ещё существует в конфиге, но даг её не запускает, и путать эти
// два состояния дорого.
export default function LinesPanel({ selected, onSelect, reloadToken }) {
  const [filter, setFilter] = useState('')
  const [scope, setScope] = useState('В работе')
  const lines = useAction(api.lines)

  useEffect(() => {
    lines.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reloadToken])

  const shown = useMemo(() => {
    const result = lines.result
    if (!result) return []
    const source = scope === 'В работе' ? result.active : result.archived
    const needle = filter.trim().toLowerCase()
    return needle
      ? source.filter((k) => k.toLowerCase().includes(needle))
      : source
  }, [lines.result, scope, filter])

  return (
    <Card
      title="Линии"
      styles={{ body: { paddingTop: 12 } }}
      extra={
        lines.result && (
          <Typography.Text type="secondary">
            всего {lines.result.all.length}
          </Typography.Text>
        )
      }
    >
      <Segmented
        block
        value={scope}
        onChange={setScope}
        options={[
          {
            label: (
              <Badge
                count={lines.result?.active.length || 0}
                offset={[10, 0]}
                color="green"
              >
                В работе
              </Badge>
            ),
            value: 'В работе',
          },
          {
            label: (
              <Badge count={lines.result?.archived.length || 0} offset={[10, 0]}>
                В архиве
              </Badge>
            ),
            value: 'В архиве',
          },
        ]}
      />

      <Input.Search
        allowClear
        placeholder="фильтр по имени"
        style={{ margin: '12px 0' }}
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
      />

      <ActionError error={lines.error} />

      {lines.loading && !lines.result ? (
        <Spin />
      ) : (
        <List
          size="small"
          bordered
          style={{ maxHeight: '60vh', overflow: 'auto' }}
          dataSource={shown}
          locale={{ emptyText: 'ничего не нашлось' }}
          renderItem={(key) => (
            <List.Item
              onClick={() => onSelect(key)}
              style={{
                cursor: 'pointer',
                background: key === selected ? '#e6f4ff' : undefined,
              }}
            >
              <Typography.Text strong={key === selected}>{key}</Typography.Text>
            </List.Item>
          )}
        />
      )}
    </Card>
  )
}
