import { useEffect, useState } from 'react'
import { Alert, Col, ConfigProvider, Layout, Row, Space, Tabs, Tag, Typography } from 'antd'

import { api } from './api'
import ruRU from 'antd/locale/ru_RU'

import LinesPanel from './LinesPanel'
import LineForm from './LineForm'
import GitBar from './GitBar'
import TriggersPage from './TriggersPage'
import VersionsPage from './VersionsPage'
import NewLinePage from './NewLinePage'
import SpPage from './SpPage'

// Оболочка. Разделы соответствуют вкладкам прежнего конструктора: сложный ETL,
// триггеры, создание с нуля.
//
// Консоли внизу здесь нет и не будет: каждое действие показывает свой
// результат и свою ошибку рядом с собой (см. useAction/ActionError). Общий
// поток вывода был главным источником «нажал и не понял, сработало ли».
export default function App() {
  const [selected, setSelected] = useState(null)
  const [creating, setCreating] = useState(false)
  const [reloadToken, setReloadToken] = useState(0)
  const [tab, setTab] = useState('lines')
  const bump = () => setReloadToken((n) => n + 1)

  // Расхождение «страница новее сервиса» — единственная поломка, которая
  // выглядит как чужая вина: клон обновился хуком, страницу браузер взял с
  // диска свежую, а python остался прежним, и первый же новый маршрут отвечает
  // 404. Спрашиваем сам сервис — он один знает, с каким деревом стартовал, — и
  // говорим об этом ДО того, как человек наткнётся на непонятную ошибку.
  const [stale, setStale] = useState(false)
  // Осиротевшие сборки в dist/assets — показание прибора, а не грязь: файл
  // сборки называется по хэшу и кладётся ровно один, значит лишние взялись
  // копированием дерева поверх вместо переноса гитом. Тем же способом НЕ
  // удаляются снесённые линии — их фрагменты останутся лежать и вернутся в
  // работу, а это уже не косметика.
  const [orphans, setOrphans] = useState([])
  useEffect(() => {
    api
      .health()
      .then((h) => {
        setStale(Boolean(h?.stale))
        setOrphans(h?.dist_orphans || [])
      })
      .catch(() => {})
  }, [reloadToken])

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
          {stale && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 8 }}
              message="Сервис конструктора старше этой страницы"
              description={
                <>
                  Рабочая копия обновилась под работающим сервисом: страница уже
                  новая, а маршруты у python остались прежние. Часть кнопок
                  ответит «нет маршрута», пока его не перезапустят.
                  <pre style={{ margin: '8px 0 0' }}>sudo systemctl restart etl-dagbuilder-api</pre>
                </>
              }
            />
          )}
          {orphans.length > 0 && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 8 }}
              message={`В сборке фронтенда ${orphans.length} лишних файлов — дерево переносили копированием`}
              description={
                <>
                  <div>
                    Сборка кладёт ровно один js и один css, имя содержит хэш.
                    Лишние файлы означают, что дерево копировали поверх, а не
                    переносили гитом: копирование добавляет и обновляет, но
                    никогда не удаляет. Работать это не мешает — но уезжает на
                    прод, а главное: тем же способом не удалятся и снесённые
                    линии, их фрагменты вернутся в работу.
                  </div>
                  <pre style={{ margin: '8px 0 0', whiteSpace: 'pre-wrap' }}>
                    {`git rm -r --cached tools/webui/dist/assets && npm --prefix tools/webui run build && git add -A`}
                  </pre>
                  <div style={{ marginTop: 8, opacity: 0.75 }}>
                    Лишние: {orphans.slice(0, 6).join(', ')}
                    {orphans.length > 6 ? ` … и ещё ${orphans.length - 6}` : ''}
                  </div>
                </>
              }
            />
          )}
          <GitBar reloadToken={reloadToken} />

          <Tabs
            activeKey={tab}
            onChange={setTab}
            style={{ marginTop: 8 }}
            items={[
              {
                key: 'lines',
                label: 'Сложный ETL',
                // Создание живёт ЗДЕСЬ, а не отдельной вкладкой наверху.
                // Раньше у сложного ETL «Линии» и «Новая линия» были разными
                // вкладками, а у справочников создание и правка — одной, и
                // одинаковые по смыслу вещи выглядели устроенными по-разному.
                // Теперь везде одинаково: слева список с кнопкой «+ Создать»,
                // справа форма — та же самая, отличается лишь тем, откуда
                // взялись начальные значения.
                children: (
                  <Row gutter={16}>
                    <Col xs={24} md={8} lg={6}>
                      <LinesPanel
                        selected={creating ? null : selected}
                        onSelect={(key) => {
                          setCreating(false)
                          setSelected(key)
                        }}
                        onCreate={() => {
                          setSelected(null)
                          setCreating(true)
                        }}
                        creating={creating}
                        reloadToken={reloadToken}
                      />
                    </Col>
                    <Col xs={24} md={16} lg={18}>
                      {creating ? (
                        <NewLinePage
                          onCancel={() => setCreating(false)}
                          onCreated={(spec) => {
                            bump()
                            setCreating(false)
                            if (spec?.line_name)
                              setSelected(
                                `${spec.line_name}${spec.db_master}${spec.db_slave}`,
                              )
                          }}
                        />
                      ) : (
                        /* key — НЕ украшение. Без него React оставляет форму
                           смонтированной при переходе на другую линию, а вместе
                           с ней и всё состояние внутри: имя в «Переименовать»,
                           готовый план переименования, список «что будет
                           удалено», незакрытый разбор снятых структур.
                           Каждое из этого относится к ПРЕЖНЕЙ линии, а кнопки
                           рядом работают уже с новой. Самое дорогое: список
                           удаления от линии A и включает кнопку, и остаётся на
                           экране — а удаляется по ней линия B.
                           key={selected} заставляет пересобрать поддерево
                           заново, и весь этот класс ошибок исчезает разом. */
                        <LineForm
                          key={selected}
                          lineKey={selected}
                          onChanged={(newKey) => {
                            bump()
                            // линию переименовали — выбираем её под новым именем
                            if (newKey) setSelected(newKey)
                          }}
                        />
                      )}
                    </Col>
                  </Row>
                ),
              },
              // Порядок — по тому, как часто и по какому поводу сюда заходят:
              // сложный ETL, справочники (обновляются по сигналу аудитного
              // триггера), разовый перенос (руками, под задачу), триггеры.
              // Справочник и разовый перенос устроены одинаково по файлам, но
              // делают их в разное время и по разным поводам — держать их на
              // одной вкладке значило показывать вперемешку несмешиваемое.
              {
                key: 'sp',
                label: 'Справочники',
                children: <SpPage kind="regular" />,
              },
              {
                key: 'once',
                label: 'Разовый перенос',
                children: <SpPage kind="once" />,
              },
              { key: 'triggers', label: 'Триггеры', children: <TriggersPage /> },
              // Последней: сюда приходят, когда работа с линиями уже сделана —
              // выложить сделанное или вернуть то, что было.
              { key: 'versions', label: 'Версии и прод', children: <VersionsPage /> },
            ]}
          />
        </Layout.Content>
      </Layout>
    </ConfigProvider>
  )
}
