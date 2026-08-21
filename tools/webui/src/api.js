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
  renamePlan: (params) => call('POST', 'rename-plan', params),
  renameApply: (params) => call('POST', 'rename-apply', params),
  archive: (key) => call('POST', 'archive', { key }),
  restore: (key) => call('POST', 'restore', { key }),
  deleteTargets: (key) => call('POST', 'delete-targets', { key }),
  deleteLine: (key) => call('POST', 'delete', { key }),
  sharedFiles: (params) => call('POST', 'shared-files', params),
  mergeTablePk: (params) => call('POST', 'merge-table-pk', params),
  gitPush: (params) => call('POST', 'git/push', params),
  // версии, откат и выкладка на прод
  gitVersions: (params) => call('GET', 'git/versions', params),
  rollbackPlan: (params) => call('POST', 'git/rollback-plan', params),
  rollbackApply: (params) => call('POST', 'git/rollback-apply', params),
  prodStatus: (params) => call('POST', 'prod/status', params),
  prodDiff: (params) => call('POST', 'prod/diff', params),
  prodDeploy: (params) => call('POST', 'prod/deploy', params),
  triggerBuild: (params) => call('POST', 'triggers/build', params),
  triggerCheck: (keys) => call('POST', 'triggers/check', { keys }),

  // справочники и разовый перенос — свой набор понятий, см. SpPage.jsx
  spLines: () => call('GET', 'sp/lines'),
  spLine: (params) => call('GET', 'sp/line', params),
  spPreview: (spec) => call('POST', 'sp/preview', { spec }),
  spParseSql: (params) => call('POST', 'sp/parse-sql', params),
  spBuildSql: (params) => call('POST', 'sp/build-sql', params),
  spRenameSelectColumn: (params) => call('POST', 'sp/rename-select-column', params),
  spMove: (params) => call('POST', 'sp/move', params),
  spDeleteTargets: (params) => call('POST', 'sp/delete-targets', params),
  spDelete: (params) => call('POST', 'sp/delete', params),
  spAuditTrigger: (params) => call('POST', 'sp/audit-trigger', params),
}
