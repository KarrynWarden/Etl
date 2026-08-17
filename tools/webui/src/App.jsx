import { useState } from 'react'
import { Col, ConfigProvider, Layout, Row, Space, Tag, Typography } from 'antd'
import ruRU from 'antd/locale/ru_RU'

import LinesPanel from './LinesPanel'
import LineForm from './LineForm'
import GitBar from './GitBar'

// Оболочка. Слева список линий, справа то, что с выбранной делают.
//
// Консоли внизу здесь нет и не будет: каждое действие показывает свой
// результат и свою ошибку рядом с собой (см. useAction/ActionError). Общий
// поток вывода был главным источником «нажал и не понял, сработало ли».
export default function App() {
  const [selected, setSelected] = useState(null)
  const [reloadToken, setReloadToken] = useState(0)

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
          <Row gutter={16} style={{ marginTop: 16 }}>
            <Col xs={24} md={8} lg={6}>
              <LinesPanel
                selected={selected}
                onSelect={setSelected}
                reloadToken={reloadToken}
              />
            </Col>
            <Col xs={24} md={16} lg={18}>
              <LineForm
                lineKey={selected}
                onChanged={() => setReloadToken((n) => n + 1)}
              />
            </Col>
          </Row>
        </Layout.Content>
      </Layout>
    </ConfigProvider>
  )
}
