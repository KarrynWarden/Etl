// Клиент API конструктора (tools/dagbuilder_api.py).
//
// Одна функция на все запросы, потому что у API один конверт ответа:
//     {"ok": true,  "result": ...}
//     {"ok": false, "error": "BadRequest", "detail": "текст"}
// Неуспех превращается в исключение с ЧИТАЕМЫМ текстом — тот, который потом
// показывается рядом с кнопкой. Ради этого весь переезд и затевался: в
// ноутбуке ошибка уезжала в конец потока вывода и терялась.

// Адрес API считается от адреса страницы, а не задаётся константой: приложение
// живёт и на '/' (режим разработки), и на '/dagbuilder/' (Apache на сервере).
const API = new URL('api/', document.baseURI).toString()

export class ApiError extends Error {
  constructor(detail, kind, status) {
    super(detail)
    this.name = 'ApiError'
    this.kind = kind
    this.status = status
  }
}

async function call(method, route, params) {
  let response
  try {
    response =
      method === 'GET'
        ? await fetch(API + route + query(params))
        : await fetch(API + route, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(params ?? {}),
          })
  } catch (err) {
    // Сеть, а не логика: сервис не запущен или упал. Текст fetch'а
    // ('Failed to fetch') сам по себе ничего не объясняет.
    throw new ApiError(
      `нет связи с сервисом конструктора (${err.message}). Запущен ли dagbuilder_api?`,
      'Network',
      0,
    )
  }

  let body
  try {
    body = await response.json()
  } catch {
    throw new ApiError(
      `сервис ответил не JSON (код ${response.status})`,
      'BadResponse',
      response.status,
    )
  }
  if (!body.ok) {
    throw new ApiError(body.detail || 'без пояснения', body.error, response.status)
  }
  return body.result
}

function query(params) {
  const entries = Object.entries(params ?? {}).filter(
    ([, v]) => v !== undefined && v !== null && v !== '',
  )
  return entries.length ? '?' + new URLSearchParams(entries).toString() : ''
}

export const api = {
  lines: () => call('GET', 'lines'),
  line: (key) => call('GET', 'line', { key }),
  tags: () => call('GET', 'tags'),
  groupDags: () => call('GET', 'group-dags'),
  gitStatus: () => call('GET', 'git/status'),
  defaults: (params) => call('GET', 'defaults', params),
  triggerTargets: () => call('GET', 'triggers/targets'),

  snapStructure: (params) => call('POST', 'snap-structure', params),
  snapQueryStructure: (params) => call('POST', 'snap-query-structure', params),
  match: (params) => call('POST', 'match', params),
  preview: (spec) => call('POST', 'preview', { spec }),
  write: (params) => call('POST', 'write', params),
  archive: (key) => call('POST', 'archive', { key }),
  restore: (key) => call('POST', 'restore', { key }),
  setFlag: (params) => call('POST', 'set-flag', params),
  deleteTargets: (key) => call('POST', 'delete-targets', { key }),
  deleteLine: (key) => call('POST', 'delete', { key }),
  sharedFiles: (params) => call('POST', 'shared-files', params),
  mergeTablePk: (params) => call('POST', 'merge-table-pk', params),
  gitPush: (params) => call('POST', 'git/push', params),
  triggerBuild: (params) => call('POST', 'triggers/build', params),
  triggerCheck: (keys) => call('POST', 'triggers/check', { keys }),
}
