import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { theme } from 'antd'

// Поле SQL с подсветкой — и с сохранением ВСЕГО поведения обычного textarea.
//
// Приём известный: подсвеченный <pre> лежит ПОД прозрачным <textarea>, оба с
// одинаковыми метриками текста и одинаковой прокруткой. Печатает человек в
// настоящий textarea — значит целиком остаются курсор, выделение, Ctrl+Z,
// автодополнение браузера, проверка орфографии, поведение Tab и мобильная
// клавиатура. Редакторы, которые рисуют курсор сами (CodeMirror, Monaco), всё
// это заменяют собственными реализациями — и вместе с ними приезжают мегабайты
// зависимостей, а сборка тут офлайновая и живёт на node 18 с Rollup 3.
//
// Что подсвечивается: ключевые слова, строки, комментарии, числа и парные
// скобки. Разбор ЛЕКСИЧЕСКИЙ, одним проходом регулярки, и он намеренно не знает
// грамматики: у нас Oracle и PostgreSQL вперемешку, а «умная» подсветка,
// ошибающаяся на чужом диалекте, хуже честной простой.
const KEYWORDS = [
  'select', 'from', 'where', 'and', 'or', 'not', 'null', 'is', 'in', 'exists',
  'between', 'like', 'as', 'on', 'join', 'inner', 'left', 'right', 'full',
  'outer', 'cross', 'group', 'order', 'by', 'having', 'union', 'all', 'minus',
  'intersect', 'except', 'distinct', 'unique', 'case', 'when', 'then', 'else',
  'end', 'insert', 'into', 'values', 'update', 'set', 'delete', 'merge',
  'using', 'matched', 'create', 'table', 'view', 'index', 'sequence', 'trigger',
  'procedure', 'function', 'begin', 'declare', 'return', 'if', 'loop', 'for',
  'with', 'over', 'partition', 'asc', 'desc', 'limit', 'offset', 'fetch',
  'first', 'rows', 'only', 'cast', 'coalesce', 'nvl', 'decode', 'trunc',
  'to_date', 'to_char', 'to_number', 'extract', 'count', 'sum', 'min', 'max',
  'avg', 'substr', 'instr', 'nullif', 'commit', 'rollback', 'truncate',
]
const KW = new Set(KEYWORDS)

// Один проход, порядок альтернатив = приоритет. Комментарии и строки идут
// первыми: внутри них ключевых слов нет, что бы там ни было написано.
const TOKENS = new RegExp(
  [
    '--[^\\n]*',                       // комментарий до конца строки
    '/\\*[\\s\\S]*?\\*/',              // блочный комментарий
    "'(?:[^']|'')*'",                  // строковый литерал (с '' внутри)
    '"[^"]*"',                         // имя в кавычках
    ':\\w+|%\\(\\w+\\)s|%s',           // плейсхолдеры драйверов
    '\\b\\d+(?:\\.\\d+)?\\b',          // числа
    '[A-Za-z_][A-Za-z0-9_$#]*',        // слово
    '[()\\[\\]]',                      // скобки
  ].join('|'),
  'g',
)

const escape = (s) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')

// Подсветка -> html. Возвращает строку, потому что вставляется через
// dangerouslySetInnerHTML: тысяча React-элементов на каждое нажатие клавиши
// заметно тормозит, а строка склеивается мгновенно. Опасности нет — весь текст
// проходит через escape, и в html попадают только наши собственные <span>.
function highlight(text, colors) {
  let out = ''
  let last = 0
  let depth = 0
  TOKENS.lastIndex = 0
  let m
  while ((m = TOKENS.exec(text)) !== null) {
    const tok = m[0]
    out += escape(text.slice(last, m.index))
    last = m.index + tok.length
    const head = tok[0]
    let color = null
    if (head === '-' || head === '/') color = colors.comment
    else if (head === "'") color = colors.string
    else if (head === '"') color = colors.name
    else if (head === ':' || head === '%') color = colors.bind
    else if (head >= '0' && head <= '9') color = colors.number
    else if (tok === '(' || tok === '[') {
      color = colors.paren[depth % colors.paren.length]
      depth += 1
    } else if (tok === ')' || tok === ']') {
      depth = Math.max(0, depth - 1)
      color = colors.paren[depth % colors.paren.length]
    } else if (KW.has(tok.toLowerCase())) color = colors.keyword
    out += color
      ? `<span style="color:${color}${color === colors.keyword ? ';font-weight:600' : ''}">${escape(tok)}</span>`
      : escape(tok)
  }
  out += escape(text.slice(last))
  // Хвостовой перенос строки <pre> не показывает, а textarea показывает: без
  // этой заглушки последняя пустая строка съезжает относительно подложки.
  return out + '\n'
}

export default function SqlArea({
  value,
  onChange,
  onBlur,
  readOnly,
  minRows = 5,
  maxRows = 20,
  style,
  ...rest
}) {
  const { token } = theme.useToken()
  const box = useRef(null)
  const area = useRef(null)
  const [height, setHeight] = useState(null)

  const colors = useMemo(
    () => ({
      keyword: token.colorPrimaryText || '#1677ff',
      string: token.colorSuccessText || '#389e0d',
      comment: token.colorTextTertiary || '#8c8c8c',
      name: token.colorWarningText || '#d46b08',
      bind: token.colorErrorText || '#cf1322',
      number: token.colorInfoText || '#0958d9',
      // Вложенность скобок цветом: в запросах справочников по три уровня
      // подзапросов, и «где закрывается эта скобка» — вопрос, который задают
      // чаще всего.
      paren: ['#c41d7f', '#08979c', '#d48806'],
    }),
    [token],
  )

  const text = value || ''
  const html = useMemo(() => highlight(text, colors), [text, colors])

  // Высота по содержимому, как autoSize у AntD: считаем по строкам, а не по
  // scrollHeight — иначе поле дёргается на каждом нажатии.
  const lineHeight = Math.round((token.fontSizeSM || 12) * 1.6)
  useLayoutEffect(() => {
    const lines = text.split('\n').length
    const rows = Math.min(maxRows, Math.max(minRows, lines))
    setHeight(rows * lineHeight + 8)
  }, [text, minRows, maxRows, lineHeight])

  // Прокрутка подложки идёт за прокруткой поля — иначе подсветка «отстаёт» от
  // текста, стоит запросу не поместиться.
  useEffect(() => {
    const el = area.current
    if (!el) return
    const sync = () => {
      if (box.current) {
        box.current.scrollTop = el.scrollTop
        box.current.scrollLeft = el.scrollLeft
      }
    }
    el.addEventListener('scroll', sync)
    return () => el.removeEventListener('scroll', sync)
  }, [])

  const shared = {
    margin: 0,
    padding: 4,
    border: 'none',
    fontFamily: 'monospace',
    fontSize: token.fontSizeSM || 12,
    lineHeight: `${lineHeight}px`,
    whiteSpace: 'pre',
    overflowWrap: 'normal',
    tabSize: 4,
  }

  return (
    <div
      style={{
        position: 'relative',
        height,
        border: `1px solid ${token.colorBorder}`,
        borderRadius: token.borderRadius,
        background: readOnly ? token.colorFillQuaternary : token.colorBgContainer,
        overflow: 'hidden',
        ...style,
      }}
    >
      <pre
        ref={box}
        aria-hidden="true"
        style={{
          ...shared,
          position: 'absolute',
          inset: 0,
          overflow: 'auto',
          pointerEvents: 'none',
          color: token.colorText,
        }}
        dangerouslySetInnerHTML={{ __html: html }}
      />
      <textarea
        ref={area}
        value={text}
        readOnly={readOnly}
        spellCheck={false}
        onChange={(e) => onChange?.(e)}
        onBlur={onBlur}
        style={{
          ...shared,
          position: 'absolute',
          inset: 0,
          width: '100%',
          height: '100%',
          resize: 'none',
          outline: 'none',
          background: 'transparent',
          // Текст прозрачный, курсор — нет: видно подложку, а печатается
          // по-настоящему сюда.
          color: 'transparent',
          caretColor: token.colorText,
          overflow: 'auto',
        }}
        {...rest}
      />
    </div>
  )
}
