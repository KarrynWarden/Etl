import { useState } from 'react'
import { Col, ConfigProvider, Layout, Row, Space, Tabs, Tag, Typography } from 'antd'
import ruRU from 'antd/locale/ru_RU'

import LinesPanel from './LinesPanel'
import LineForm from './LineForm'
import GitBar from './GitBar'
import TriggersPage from './TriggersPage'
import NewLinePage from './NewLinePage'

// Оболочка. Разделы соответствуют вкладкам прежнего конструктора: сложный ETL,
// триггеры, создание с нуля.
//
// Консоли внизу здесь нет и не будет: каждое действие показывает свой
// результат и свою ошибку рядом с собой (см. useAction/ActionError). Общий
// поток вывода был главным источником «нажал и не понял, сработало ли».
export default function App() {
  const [selected, setSelected] = useState(null)
  const [reloadToken, setReloadToken] = useState(0)
  const [tab, setTab] = useState('lines')
  const bump = () => setReloadToken((n) => n + 1)

  return (
    <ConfigProvider locale={ruRU}>
      <Layout style={{ minHeight: '100vh' }}>
        <Layout.Header style={{ background: '#fff', borderBottom: '1px solid #f0f0f0' }}>
          <Space size="middle">
            <Typography.Title level={4} style={{ margin: 0 }}>
              Конструктор ETL-линий
            </Typography.Title>
            <Tag color="blue">Orcl ↔ Post</Tag>
          </Space>
        </Layout.Header>

        <Layout.Content style={{ padding: 16 }}>
          <GitBar reloadToken={reloadToken} />

          <Tabs
            activeKey={tab}
            onChange={setTab}
            style={{ marginTop: 8 }}
            items={[
              {
                key: 'lines',
                label: 'Линии',
                children: (
                  <Row gutter={16}>
                    <Col xs={24} md={8} lg={6}>
                      <LinesPanel
                        selected={selected}
                        onSelect={setSelected}
                        reloadToken={reloadToken}
                      />
                    </Col>
                    <Col xs={24} md={16} lg={18}>
                      <LineForm lineKey={selected} onChanged={bump} />
                    </Col>
                  </Row>
                ),
              },
              {
                key: 'new',
                label: 'Новая линия',
                children: (
                  <NewLinePage
                    onCreated={(spec) => {
                      bump()
                      if (spec?.line_name) {
                        setSelected(`${spec.line_name}${spec.db_master}${spec.db_slave}`)
                        setTab('lines')
                      }
                    }}
                  />
                ),
              },
              { key: 'triggers', label: 'Триггеры', children: <TriggersPage /> },
            ]}
          />
        </Layout.Content>
      </Layout>
    </ConfigProvider>
  )
}
