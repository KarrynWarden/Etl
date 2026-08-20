import { Space, Switch, Tooltip, Typography } from 'antd'
import { LockOutlined, UnlockOutlined } from '@ant-design/icons'

// Замок на Select.sql — ОДИН переключатель, показанный в двух местах: на
// вкладке SQL (где лежит сам запрос) и на вкладке «Колонки» (где его правят,
// сами того не замечая, — переименованием и регистром). Дублируется намеренно:
// запирать приходят из SQL, а натыкаются на замок в колонках, и искать кнопку
// на соседней вкладке — работа, которой быть не должно.
//
// Запертый запрос значит ровно две вещи, и обе про текст:
//   * конструктор его не пересобирает (то самое «запрос мой»);
//   * интерфейс его не правит — ни переименование колонки, ни регистр, ни
//     ручной ввод в поле.
// Единственное исключение — удаление колонки: она обязана уйти и из запроса,
// иначе рантайм упадёт на привязке (см. sp_builder.remove_select_column).
export default function SqlLock({ locked, onChange, note }) {
  return (
    <Space size={8} wrap>
      <Tooltip
        title={
          locked
            ? 'Снять замок: запрос снова можно править — руками, переименованием колонок и приведением регистра'
            : 'Запереть: текст запроса станет вашим и перестанет меняться от правок колонок'
        }
      >
        <Switch
          checked={Boolean(locked)}
          onChange={onChange}
          checkedChildren={<LockOutlined />}
          unCheckedChildren={<UnlockOutlined />}
        />
      </Tooltip>
      <span>{locked ? 'Select.sql заперт' : 'Select.sql открыт'}</span>
      {note && (
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {locked
            ? 'правится только удалением колонки — она обязана уйти и из запроса'
            : 'переименование колонок и регистр меняют текст запроса'}
        </Typography.Text>
      )}
    </Space>
  )
}
