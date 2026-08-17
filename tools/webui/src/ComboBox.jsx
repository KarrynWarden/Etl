import { AutoComplete } from 'antd'

// Выпадающий список, в который можно ВПИСАТЬ своё значение.
//
// В старом интерфейсе этого не было: ipywidgets умеет либо Dropdown с
// фиксированным набором, либо текстовое поле, и «своё значение» приходилось
// доделывать отдельной кнопкой ✎, переключавшей одно на другое. Здесь это
// штатное поведение AutoComplete: набор подсказывается, любой другой текст
// принимается как есть.
//
// Применяется там, где список ЗНАЕМ не полностью: колонка периода (обычно
// createdate, но бывает dcalc или своя), тип колонки (частые собраны, редкие
// вписываются), имя составного дага.
export default function ComboBox({ value, onChange, options, placeholder, ...rest }) {
  const items = (options || []).map((o) =>
    typeof o === 'string' ? { value: o } : o,
  )
  return (
    <AutoComplete
      value={value ?? ''}
      onChange={onChange}
      options={items}
      placeholder={placeholder}
      allowClear
      // Подсказки фильтруются по подстроке без учёта регистра: имена колонок
      // приезжают из Oracle ВЕРХНИМ регистром, из Postgres — нижним, и искать
      // с учётом регистра здесь бессмысленно.
      filterOption={(input, option) =>
        String(option?.value ?? '').toLowerCase().includes(input.toLowerCase())
      }
      {...rest}
    />
  )
}
