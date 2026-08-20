import { Button, Input, Space, Tooltip, Typography } from 'antd'
import { FontSizeOutlined, SwapOutlined } from '@ant-design/icons'

import { toDbCase, matchesDbCase, carryName } from './dbCase'

// Поле имени таблицы с приведением регистра к диалекту БД — ОТДЕЛЬНОЙ КНОПКОЙ.
//
// Кнопка, а не правка на лету, намеренно. Имя таблицы человек набирает, вставляет
// из буфера, правит по частям, — и поле, которое переписывает набранное на
// каждой букве, отбирает контроль ровно тогда, когда он нужнее всего. Здесь
// видно, что регистр не тот, и приведение делается по нажатию.
//
// Само поле не трогает и значение, пришедшее с диска. У восемнадцати линий
// справочников ведомая записана не тем регистром и прекрасно работает (для
// неэкранированных имён обе БД регистр игнорируют); перебей их при открытии
// формы — и предпросмотр покажет правку, которой человек не делал.
//
// Почему регистр вообще важен, раз БД его игнорируют: из имени таблицы
// складываются имя линии (tableNameEtlJobs) и пути структур, а имя линии
// сравнивается с etl_jobs.tablename ДОСЛОВНО. Ошибка здесь — это «Конфиг для
// ключа ... не найден» при переносе и молчаливый ноль групп в аудите.
export default function TableNameInput({ value, db, onChange, carryFrom, carryLabel, ...rest }) {
  const fixed = toDbCase(value, db)
  const off = Boolean(value) && !matchesDbCase(value, db)
  const want = db === 'Orcl' ? 'ВЕРХНЕМУ' : 'нижнему'
  // Перенос имени с другой стороны. В корпусе имя совпадает у 69 линий из 87,
  // поэтому набирать его дважды — работа на ровном месте.
  const carried = carryFrom ? carryName(carryFrom, value, db) : ''
  const canCarry = Boolean(carried) && carried !== (value || '')

  return (
    <Space direction="vertical" size={2} style={{ display: 'flex' }}>
      <Space.Compact style={{ width: '100%' }}>
        <Input value={value || ''} onChange={(e) => onChange(e.target.value)} {...rest} />
        {carryFrom !== undefined && (
          <Tooltip
            title={
              !carryFrom
                ? `Сначала заполните: ${carryLabel}`
                : canCarry
                  ? `Взять имя из «${carryLabel}»: ${carryFrom} → ${carried}`
                  : `Имя уже совпадает с «${carryLabel}»`
            }
          >
            <Button
              icon={<SwapOutlined />}
              disabled={!canCarry}
              onClick={() => onChange(carried)}
            />
          </Tooltip>
        )}
        <Tooltip
          title={
            off
              ? `Привести к ${want} регистру: ${fixed}`
              : `Регистр уже соответствует ${db === 'Orcl' ? 'Oracle' : 'Postgres'}`
          }
        >
          <Button
            icon={<FontSizeOutlined />}
            disabled={!off}
            danger={off}
            onClick={() => onChange(fixed)}
          />
        </Tooltip>
      </Space.Compact>
      {off && (
        <Typography.Text type="warning" style={{ fontSize: 12 }}>
          {db === 'Orcl' ? 'Oracle' : 'Postgres'} — имена {want} регистром, будет{' '}
          <Typography.Text code>{fixed}</Typography.Text>
        </Typography.Text>
      )}
    </Space>
  )
}
