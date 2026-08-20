import { useEffect, useRef, useState } from 'react'
import { Col, Form, Input, Row, Space, Switch, Tooltip, Typography } from 'antd'

import { toDbCase, bare, carryName } from './dbCase'

// Пара полей «ведущая таблица / ведомая таблица».
//
// Два правила, оба вытекают из устройства проекта, а не из вкуса:
//
// 1. РЕГИСТР ДЕРЖИТСЯ САМ. Oracle — ВЕРХНИЙ, Postgres — нижний, всегда.
//    Раз правило постоянное, кнопка для него была лишней работой: набрал
//    строчными в поле Oracle — в поле оказывается ВЕРХНИЙ, и думать не о чем.
//    Содержимое кавычек не трогается: в `sch."MixedCase"` регистр значим, и
//    незакрытая кавычка тоже считается кавычкой — иначе набор такого имени
//    портился бы на полпути.
//
// 2. СИНХРОНИЗАЦИЯ. В корпусе имя совпадает у 69 линий из 87, так что чаще
//    всего второе поле — это первое, только в другом регистре. При включённом
//    переключателе набор в любом из полей заполняет оба, каждое в своём
//    диалекте.
//
//    Переносится ИМЯ БЕЗ СХЕМЫ: «совпадает» здесь про имя, а не про строку
//    целиком, и схемы у сторон сплошь разные — KOKNAEV.MEDREE_PRDISP →
//    medree_prdisp, iaddress → KOKNAEV.IADDRESS. Схема, уже набранная в поле,
//    остаётся своя.
//
//    Включена по умолчанию, только если имена И ТАК совпадают (или полей ещё
//    не заполняли). У восемнадцати линий они разные по делу — PLANOMS →
//    tpplanoms, EXPMED → mocheck, — и включать там синхронизацию значило бы
//    предлагать испортить рабочую линию.
const sameName = (a, b) =>
  bare(a).toLowerCase() === bare(b).toLowerCase()

export default function TableNamePair({
  masterValue,
  masterDb,
  masterLabel = 'Ведущая таблица',
  masterHelp,
  onMasterChange,
  slaveValue,
  slaveDb,
  slaveLabel = 'Ведомая таблица',
  slaveHelp,
  onSlaveChange,
  resetKey,
}) {
  const both = Boolean(masterValue) && Boolean(slaveValue)
  const [sync, setSync] = useState(!both || sameName(masterValue, slaveValue))

  // Другая линия — другой ответ на вопрос «синхронизировать ли». Считаем его
  // заново при смене линии, а не один раз при первой отрисовке.
  const initial = useRef(resetKey)
  useEffect(() => {
    if (initial.current === resetKey) return
    initial.current = resetKey
    const filled = Boolean(masterValue) && Boolean(slaveValue)
    setSync(!filled || sameName(masterValue, slaveValue))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resetKey])

  // Смена БД меняет и требуемый регистр — выравниваем сразу, не дожидаясь,
  // пока человек тронет поле. Иначе после переключения Oracle→Postgres в поле
  // осталось бы имя чужого диалекта.
  useEffect(() => {
    const fixed = toDbCase(masterValue, masterDb)
    if (masterValue && fixed !== masterValue) onMasterChange(fixed)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [masterDb])
  useEffect(() => {
    const fixed = toDbCase(slaveValue, slaveDb)
    if (slaveValue && fixed !== slaveValue) onSlaveChange(fixed)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [slaveDb])

  const typeMaster = (raw) => {
    const value = toDbCase(raw, masterDb)
    onMasterChange(value)
    if (sync) onSlaveChange(carryName(value, slaveValue, slaveDb))
  }
  const typeSlave = (raw) => {
    const value = toDbCase(raw, slaveDb)
    onSlaveChange(value)
    if (sync) onMasterChange(carryName(value, masterValue, masterDb))
  }

  // Включение синхронизации само по себе ничего не переписывает: человек
  // включает её, чтобы дальше набирать одно имя, а не чтобы одно из уже
  // введённых значений молча исчезло. Выравнивание — первым же набором.
  const differ = both && !sameName(masterValue, slaveValue)

  return (
    <>
      <Row gutter={16} align="middle" style={{ marginBottom: 4 }}>
        <Col>
          <Space size={8}>
            <Switch size="small" checked={sync} onChange={setSync} />
            <Tooltip
              title={
                sync
                  ? 'Набор в любом из полей заполняет оба: имя одно, регистр — по диалекту каждой БД. Схема у каждой стороны своя.'
                  : 'Поля независимы. Регистр всё равно держится сам: Oracle — ВЕРХНИЙ, Postgres — нижний.'
              }
            >
              <Typography.Text type={sync ? undefined : 'secondary'}>
                имя таблицы одно на обе стороны
              </Typography.Text>
            </Tooltip>
            {sync && differ && (
              <Typography.Text type="warning" style={{ fontSize: 12 }}>
                сейчас имена разные — выровняются, как только начнёте набирать
              </Typography.Text>
            )}
          </Space>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label={`${masterLabel} (${masterDb})`} help={masterHelp}>
            <Input
              value={masterValue || ''}
              onChange={(e) => typeMaster(e.target.value)}
            />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label={`${slaveLabel} (${slaveDb})`} help={slaveHelp}>
            <Input
              value={slaveValue || ''}
              onChange={(e) => typeSlave(e.target.value)}
            />
          </Form.Item>
        </Col>
      </Row>
    </>
  )
}
