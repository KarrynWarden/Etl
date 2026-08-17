import { useEffect, useMemo, useState } from 'react'
import { Button, Card, Empty, Input, List, Select, Space, Spin, Tag, Tooltip } from 'antd'
import { ReloadOutlined } from '@ant-design/icons'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'

// Список линий с фильтрами.
//
// Фильтры не украшение: линий три десятка, и искать в них глазами по имени
// работает, только пока помнишь имя. Реальные вопросы к списку другие — «что
// у нас идёт из Oracle в Postgres», «где режим iud», «что помечено тегом
// mocheck», «что лежит в архиве». Поэтому фильтров четыре, и они складываются.
const ARCHIVE = { any: 'Все', work: 'В работе', archived: 'В архиве' }

export default function LinesPanel({ selected, onSelect, reloadToken }) {
  const [text, setText] = useState('')
  const [direction, setDirection] = useState(null)
  const [mode, setMode] = useState(null)
  const [tags, setTags] = useState([])
  const [archive, setArchive] = useState('work')

  const lines = useAction(api.lines)

  useEffect(() => {
    lines.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reloadToken])

  const shown = useMemo(() => {
    const all = lines.result?.lines || []
    const needle = text.trim().toLowerCase()
    return all.filter((l) => {
      if (archive === 'work' && l.archived) return false
      if (archive === 'archived' && !l.archived) return false
      if (direction && l.direction !== direction) return false
      if (mode && l.mode !== mode) return false
      if (tags.length && !tags.every((t) => l.tags.includes(t))) return false
      if (
        needle &&
        !`${l.key} ${l.table_master} ${l.table_slave}`.toLowerCase().includes(needle)
      )
        return false
      return true
    })
  }, [lines.result, text, direction, mode, tags, archive])

  const total = lines.result?.lines.length || 0
  const dirty = direction || mode || tags.length || text || archive !== 'work'

  return (
    <Card
      title="Линии"
      size="small"
      extra={
        <Space>
          <Tooltip title="Перечитать список">
            <Button
              size="small"
              icon={<ReloadOutlined />}
              loading={lines.loading}
              onClick={() => lines.run()}
            />
          </Tooltip>
        </Space>
      }
    >
      <Space direction="vertical" size={8} style={{ display: 'flex' }}>
        <Input.Search
          allowClear
          placeholder="имя линии или таблицы"
          value={text}
          onChange={(e) => setText(e.target.value)}
        />
        <Space.Compact style={{ width: '100%' }}>
          <Select
            style={{ width: '50%' }}
            allowClear
            placeholder="направление"
            value={direction}
            onChange={setDirection}
            options={(lines.result?.directions || []).map((d) => ({ value: d }))}
          />
          <Select
            style={{ width: '50%' }}
            allowClear
            placeholder="режим"
            value={mode}
            onChange={setMode}
            options={(lines.result?.modes || []).map((m) => ({ value: m }))}
          />
        </Space.Compact>
        <Select
          mode="multiple"
          allowClear
          placeholder="теги дага"
          value={tags}
          onChange={setTags}
          options={(lines.result?.tags || []).map((t) => ({ value: t }))}
        />
        <Select
          value={archive}
          onChange={setArchive}
          options={Object.entries(ARCHIVE).map(([value, label]) => ({ value, label }))}
        />
        <Space>
          <span style={{ color: '#888' }}>
            показано {shown.length} из {total}
          </span>
          {dirty && (
            <Button
              size="small"
              type="link"
              onClick={() => {
                setText('')
                setDirection(null)
                setMode(null)
                setTags([])
                setArchive('work')
              }}
            >
              сбросить фильтры
            </Button>
          )}
        </Space>
      </Space>

      <ActionError error={lines.error} />

      {lines.loading && !lines.result ? (
        <Spin style={{ marginTop: 16 }} />
      ) : (
        <List
          size="small"
          bordered
          style={{ marginTop: 8, maxHeight: '52vh', overflow: 'auto' }}
          dataSource={shown}
          locale={{ emptyText: <Empty description="ничего не нашлось" /> }}
          renderItem={(l) => (
            <List.Item
              onClick={() => onSelect(l.key)}
              style={{
                cursor: 'pointer',
                background: l.key === selected ? '#e6f4ff' : undefined,
                display: 'block',
              }}
            >
              <div>
                <b>{l.key}</b>
              </div>
              <Space size={4} wrap style={{ marginTop: 4 }}>
                <Tag>{l.mode}</Tag>
                {l.group_dag && <Tag color="blue">в составном</Tag>}
                {l.archived && <Tag color="default">архив</Tag>}
                {l.disabled && <Tag color="red">отключена</Tag>}
                {l.skip_audit && <Tag color="orange">без аудита</Tag>}
              </Space>
            </List.Item>
          )}
        />
      )}
    </Card>
  )
}
