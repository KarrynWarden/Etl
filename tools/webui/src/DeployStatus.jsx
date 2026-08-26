import { useEffect, useRef, useState } from 'react'
import { Button, Space, Tag, Tooltip, Typography } from 'antd'

import { api } from './api'

// Когда сервисы перезапустились на самом деле.
//
// «Заявка на перезапуск подана» — формулировка честная, но человеку от неё мало
// толку: он хочет знать не что попросили, а что произошло. Ответить на это в
// выводе push'а нельзя в принципе — перезапуск случается ПОСЛЕ того, как хук
// закончился, а конструктор при этом перезапускает сам себя, то есть убивает
// процесс, в котором хук и работал.
//
// Поэтому смотрим с другой стороны: когда юниты стартовали. Старт позже заявки
// — перезапуск состоялся.
//
// Опрос идёт ЧАСТО, пока ждём, и редко, когда ждать нечего. Постоянный опрос
// раз в пять секунд — это тысячи запросов за рабочий день ради события, которое
// случается несколько раз.
const WAIT_MS = 3000
const IDLE_MS = 60000

function hhmmss(seconds) {
  if (!seconds) return '—'
  const d = new Date(seconds * 1000)
  return d.toLocaleTimeString('ru-RU', { hour12: false })
}

function ago(seconds, now) {
  if (!seconds || !now) return ''
  const s = Math.max(0, Math.round(now - seconds))
  if (s < 60) return `${s} с назад`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} мин назад`
  const h = Math.round(m / 60)
  return h < 24 ? `${h} ч назад` : `${Math.round(h / 24)} дн назад`
}

const LABEL = {
  'airflow-test-scheduler': 'планировщик',
  'airflow-test-webserver': 'веб airflow',
  'etl-dagbuilder-api': 'конструктор',
}

export default function DeployStatus({ reloadToken }) {
  const [data, setData] = useState(null)
  const [busy, setBusy] = useState(false)
  const timer = useRef(null)

  const load = async () => {
    setBusy(true)
    try {
      setData(await api.deployStatus())
    } catch {
      // Маршрута может не быть — сервис старше страницы. Про это уже говорит
      // отдельное предупреждение в шапке, второй раз пугать незачем.
      setData(null)
    } finally {
      setBusy(false)
    }
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reloadToken])

  useEffect(() => {
    clearTimeout(timer.current)
    if (!data) return undefined
    timer.current = setTimeout(load, data.pending ? WAIT_MS : IDLE_MS)
    return () => clearTimeout(timer.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data])

  if (!data) return null

  const known = (data.units || []).filter((u) => u.started)
  const newest = known.length ? Math.max(...known.map((u) => u.started)) : null

  return (
    <Space size={4} wrap>
      <Typography.Text type="secondary">перезапуск</Typography.Text>
      {data.pending ? (
        <Tag color="processing">
          запрошен в {hhmmss(data.requested)} — ждём…
        </Tag>
      ) : (
        <Tooltip
          title={
            <div>
              {(data.units || []).map((u) => (
                <div key={u.name}>
                  {LABEL[u.name] || u.name}:{' '}
                  {u.started ? hhmmss(u.started) : 'юнита нет / не видно'}
                </div>
              ))}
              {data.requested ? (
                <div style={{ marginTop: 6, opacity: 0.8 }}>
                  последняя заявка: {hhmmss(data.requested)}
                </div>
              ) : null}
            </div>
          }
        >
          <Tag color={newest ? 'green' : 'default'}>
            {newest ? `${hhmmss(newest)} (${ago(newest, data.now)})` : 'сервисы не видны'}
          </Tag>
        </Tooltip>
      )}
      <Button size="small" type="text" loading={busy} onClick={load}>
        обновить
      </Button>
    </Space>
  )
}
