import { Component } from 'react'
import { Alert, Button, Space, Typography } from 'antd'

// Последний рубеж: ошибка в отрисовке не должна оставлять ПУСТУЮ страницу.
//
// React 18 при исключении в render снимает всё дерево целиком — окно белое, ни
// строчки текста, и по виду не отличить «сервис не отвечает» от «в данных
// что-то, чего интерфейс не ждал». Именно так это и выглядело: страница
// перестала открываться совсем, в другом браузере тоже, и виноватым назначился
// сервер, который на самом деле работал.
//
// Здесь падение превращается в читаемое сообщение с текстом ошибки и кнопкой
// перезагрузки. Стек кладём в details: он нужен, но не в лицо.
export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null, info: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    this.setState({ info })
    // eslint-disable-next-line no-console
    console.error('Конструктор упал при отрисовке:', error, info)
  }

  render() {
    const { error, info } = this.state
    if (!error) return this.props.children
    return (
      <div style={{ padding: 24, maxWidth: 900, margin: '0 auto' }}>
        <Alert
          type="error"
          showIcon
          message="Интерфейс конструктора упал"
          description={
            <Space direction="vertical" style={{ display: 'flex' }}>
              <Typography.Text>
                Данные на диске не пострадали: страница падает при отрисовке, до
                записи дело не доходит. Ошибка ниже — то, что нужно показать
                разработчику.
              </Typography.Text>
              <Typography.Text code>{String(error?.message || error)}</Typography.Text>
              <details>
                <summary>подробности</summary>
                <pre style={{ overflow: 'auto', maxHeight: 320 }}>
                  {String(error?.stack || '')}
                  {String(info?.componentStack || '')}
                </pre>
              </details>
              <Space>
                <Button type="primary" onClick={() => window.location.reload()}>
                  Перезагрузить страницу
                </Button>
                <Button onClick={() => this.setState({ error: null, info: null })}>
                  Попробовать отрисовать заново
                </Button>
              </Space>
            </Space>
          }
        />
      </div>
    )
  }
}
