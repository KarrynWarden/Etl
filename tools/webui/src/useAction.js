import { useCallback, useState } from 'react'

// Один вызов API = состояние {loading, error, result}, привязанное к МЕСТУ,
// откуда его запустили.
//
// Это ответ на главную претензию к старому интерфейсу: ошибка «Снять структуры
// из БД» печаталась в общую консоль внизу страницы, и её можно было просто не
// заметить. Здесь ошибка остаётся у своей кнопки и живёт ровно до следующего
// запуска того же действия.
export function useAction(fn) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [result, setResult] = useState(null)

  const run = useCallback(
    async (...args) => {
      setLoading(true)
      setError(null)
      try {
        const value = await fn(...args)
        setResult(value)
        return value
      } catch (err) {
        setError(err)
        return undefined // вызывающему не нужно ловить: он смотрит на error
      } finally {
        setLoading(false)
      }
    },
    [fn],
  )

  return {
    run,
    loading,
    error,
    result,
    // reset — убрать ТОЛЬКО ошибку: результат при этом остаётся на экране.
    reset: () => setError(null),
    // clear — убрать и результат: нужно там, где показанное перестало быть
    // правдой. Предпросмотр после пуша — ровно этот случай: файлы уже уехали,
    // а список «что изменится» продолжает висеть и предлагать записать.
    clear: () => {
      setError(null)
      setResult(null)
    },
  }
}
