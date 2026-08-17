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
  UnknownRoute: 'Устаревший интерфейс',
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
      description={error.message}
    />
  )
}
