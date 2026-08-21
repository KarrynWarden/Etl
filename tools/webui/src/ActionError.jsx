import { Alert } from 'antd'

// Ошибка действия — ровно там, где действие запустили.
//
// Показываем и вид ошибки, и текст: 'BadRequest' означает «неверно заполнена
// форма» (чинится тут же), 'Network' — «сервис не отвечает» (чинится на
// сервере), остальное — ошибка логики конструктора. Разные ошибки требуют
// разных действий, и различать их должен пользователь, а не разработчик по
// логам.
const TITLES = {
  BadRequest: 'Не хватает данных',
  Network: 'Нет связи с сервисом',
  BadResponse: 'Неожиданный ответ сервиса',
  // Раньше здесь было «Устаревший интерфейс» — и это ровно наоборот.
  // Страницу браузер берёт с диска, поэтому она всегда свежая; старым
  // остаётся python, который запустился до обновления клона и знать не
  // знает про новые маршруты. Чинится рестартом сервиса, а не браузером,
  // и подсказать надо именно это.
  UnknownRoute: 'Сервис конструктора старше страницы — нужен перезапуск',
}

export default function ActionError({ error, onClose }) {
  if (!error) return null
  return (
    <Alert
      style={{ marginTop: 12 }}
      type="error"
      showIcon
      closable={Boolean(onClose)}
      onClose={onClose}
      message={TITLES[error.kind] || `Ошибка: ${error.kind || 'неизвестная'}`}
      description={
        error.kind === 'UnknownRoute' ? (
          <>
            <div>
              Страница обновилась вместе с клоном, а работающий python остался
              прежним — в нём этого маршрута ещё нет.
            </div>
            <pre style={{ margin: '8px 0 0' }}>sudo systemctl restart etl-dagbuilder-api</pre>
            <div style={{ marginTop: 8, opacity: 0.75 }}>{error.message}</div>
          </>
        ) : (
          error.message
        )
      }
    />
  )
}
