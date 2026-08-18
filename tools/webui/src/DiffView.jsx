import { useMemo } from 'react'
import { Typography } from 'antd'

// Построчная разница «что лежит на диске» → «что запишется».
//
// До этого предпросмотр показывал только НОВЫЙ текст целиком. Для дага в двести
// строк это значит вычитывать их глазами в поисках правки, которая на деле в
// одной строке, — а решение «писать или не писать этот файл» принимается именно
// по ней.
//
// Алгоритм — обычный LCS по строкам (тот же, что у diff): совпадающие строки
// показываются серым как контекст, удалённые красным с «−», добавленные зелёным
// с «+». Файлы здесь маленькие (даг, конфиг, структура), поэтому квадратичная
// таблица дешевле любых ухищрений; на всякий случай есть предел, за которым
// показывается просто новый текст.
const MAX_LINES = 4000
const CONTEXT = 3

function lcsTable(a, b) {
  const n = a.length
  const m = b.length
  // одна плоская типизированная таблица вместо массива массивов
  const t = new Int32Array((n + 1) * (m + 1))
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      t[i * (m + 1) + j] =
        a[i] === b[j]
          ? t[(i + 1) * (m + 1) + j + 1] + 1
          : Math.max(t[(i + 1) * (m + 1) + j], t[i * (m + 1) + j + 1])
    }
  }
  return t
}

// [{kind: 'same'|'del'|'add', text, oldNo, newNo}]
function diffLines(oldText, newText) {
  const a = oldText.split('\n')
  const b = newText.split('\n')
  const out = []
  if (a.length + b.length > MAX_LINES) {
    b.forEach((text, i) => out.push({ kind: 'same', text, newNo: i + 1 }))
    return out
  }
  const m = b.length
  const t = lcsTable(a, b)
  let i = 0
  let j = 0
  while (i < a.length && j < m) {
    if (a[i] === b[j]) {
      out.push({ kind: 'same', text: a[i], oldNo: i + 1, newNo: j + 1 })
      i++
      j++
    } else if (t[(i + 1) * (m + 1) + j] >= t[i * (m + 1) + j + 1]) {
      out.push({ kind: 'del', text: a[i], oldNo: i + 1 })
      i++
    } else {
      out.push({ kind: 'add', text: b[j], newNo: j + 1 })
      j++
    }
  }
  for (; i < a.length; i++) out.push({ kind: 'del', text: a[i], oldNo: i + 1 })
  for (; j < m; j++) out.push({ kind: 'add', text: b[j], newNo: j + 1 })
  return out
}

// Свернуть длинные куски без изменений: в даге правится одна строка, а строк
// двести, и без этого разницу приходится искать прокруткой.
function collapse(rows) {
  const keep = new Array(rows.length).fill(false)
  rows.forEach((r, i) => {
    if (r.kind === 'same') return
    for (let k = Math.max(0, i - CONTEXT); k <= Math.min(rows.length - 1, i + CONTEXT); k++)
      keep[k] = true
  })
  const out = []
  let hidden = 0
  rows.forEach((r, i) => {
    if (keep[i]) {
      if (hidden) {
        out.push({ kind: 'gap', text: `… ${hidden} строк без изменений …` })
        hidden = 0
      }
      out.push(r)
    } else {
      hidden++
    }
  })
  if (hidden) out.push({ kind: 'gap', text: `… ${hidden} строк без изменений …` })
  return out
}

const STYLE = {
  same: { background: 'transparent', color: '#555' },
  del: { background: '#fff1f0', color: '#a8071a' },
  add: { background: '#f6ffed', color: '#135200' },
  gap: { background: '#fafafa', color: '#999', fontStyle: 'italic' },
}
const SIGN = { same: ' ', del: '−', add: '+', gap: ' ' }

export default function DiffView({ oldText, newText, maxHeight = 360 }) {
  const rows = useMemo(() => {
    if (oldText === null || oldText === undefined)
      return newText.split('\n').map((text, i) => ({ kind: 'add', text, newNo: i + 1 }))
    return collapse(diffLines(oldText, newText))
  }, [oldText, newText])

  const added = rows.filter((r) => r.kind === 'add').length
  const removed = rows.filter((r) => r.kind === 'del').length

  return (
    <div>
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        строк добавлено {added}, убрано {removed}
      </Typography.Text>
      <pre
        style={{
          maxHeight,
          overflow: 'auto',
          margin: '4px 0 0',
          padding: 0,
          fontSize: 12,
          lineHeight: 1.5,
          border: '1px solid #f0f0f0',
          borderRadius: 4,
        }}
      >
        {rows.map((r, i) => (
          <div key={i} style={{ ...STYLE[r.kind], padding: '0 8px', whiteSpace: 'pre-wrap' }}>
            <span style={{ display: 'inline-block', width: 46, color: '#bbb', userSelect: 'none' }}>
              {r.oldNo ?? ''}
              {' '}
              {r.newNo ?? ''}
            </span>
            <span style={{ userSelect: 'none' }}>{SIGN[r.kind]} </span>
            {r.text}
          </div>
        ))}
      </pre>
    </div>
  )
}
