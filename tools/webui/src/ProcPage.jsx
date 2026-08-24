import { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Col,
  Form,
  Input,
  InputNumber,
  List,
  Row,
  Select,
  Space,
  Tag,
  Typography,
} from 'antd'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import CodeArea from './CodeArea'
import FilesPreview from './FilesPreview'

// Даги-процедуры. Здесь нет ни структур, ни сопоставления колонок — есть
// расписание и КОД, который человек пишет сам.
//
// Отсюда всё устройство страницы: слева список, справа форма обвязки и под ней
// редактор тела функции. Форма владеет только обвязкой; тело уходит на диск
// дословно, конструктор в него не заглядывает.
//
// Даг, написанный руками (A56*, A61*), формой НЕ правится: у него нет маркера,
// и страница показывает его целиком, только для чтения. Двести строк логики с
// курсорами по двум базам форма не опишет, а перезапись «тем, что форма знает»
// стёрла бы остальное.
const { Paragraph, Text } = Typography

const SCHEDULE_HINTS = [
  { value: 'dt.timedelta(minutes=10)', label: 'каждые 10 минут' },
  { value: 'dt.timedelta(minutes=60)', label: 'раз в час' },
  { value: 'dt.timedelta(days=1)', label: 'раз в сутки' },
  { value: "'50 5,7,13 * * *'", label: 'cron: 5:50, 7:50, 13:50' },
  { value: "'0 3 * * *'", label: 'cron: каждый день в 3:00' },
  { value: 'None', label: 'только вручную' },
]

// Расписание в файле — выражение python, и в списке оно читается как код:
// `dt.timedelta(minutes=10)`. Человеку нужно «каждые 10 минут», поэтому
// переводим то, что переводится, а незнакомое показываем как есть — врать про
// расписание хуже, чем показать код.
function humanSchedule(expr) {
  const raw = (expr || '').trim()
  if (!raw || raw === 'None') return 'только вручную'
  const td = raw.match(/^dt\.timedelta\(\s*(\w+)\s*=\s*(\d+)\s*\)$/)
  if (td) {
    const [, unit, n] = td
    const num = Number(n)
    const word = (one, few, many) => {
      const mod10 = num % 10
      const mod100 = num % 100
      if (mod10 === 1 && mod100 !== 11) return one
      if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return few
      return many
    }
    if (unit === 'minutes')
      return num === 1 ? 'каждую минуту' : `каждые ${num} ${word('минуту', 'минуты', 'минут')}`
    if (unit === 'hours')
      return num === 1 ? 'раз в час' : `каждые ${num} ${word('час', 'часа', 'часов')}`
    if (unit === 'days')
      return num === 1 ? 'раз в сутки' : `каждые ${num} ${word('сутки', 'суток', 'суток')}`
  }
  const cron = raw.match(/^['"](.+)['"]$/)
  if (cron) return `cron ${cron[1]}`
  return raw
}

export default function ProcPage() {
  const [selected, setSelected] = useState(null)
  const [creating, setCreating] = useState(false)
  const [spec, setSpec] = useState(null)
  const [alien, setAlien] = useState(null)
  const [reloadToken, setReloadToken] = useState(0)

  const list = useAction(api.procList)
  const load = useAction(api.procLine)
  const blank = useAction(api.procDefaults)
  const preview = useAction(api.procPreview)
  const write = useAction(api.write)

  useEffect(() => {
    list.run()
  }, [reloadToken])

  const open = async (dagId) => {
    setCreating(false)
    setSelected(dagId)
    preview.reset()
    const got = await load.run(dagId)
    if (!got) return
    if (got.generated) {
      setAlien(null)
      setSpec(got)
    } else {
      // Чужой файл в форму не кладём вовсе: пустая форма рядом с его текстом —
      // приглашение нажать «Записать» и потерять всё, чего форма не знает.
      setSpec(null)
      setAlien(got)
    }
  }

  const create = async () => {
    setSelected(null)
    setAlien(null)
    preview.reset()
    const got = await blank.run()
    if (got) {
      setCreating(true)
      setSpec(got)
    }
  }

  const set = (patch) => setSpec((prev) => ({ ...prev, ...patch }))

  const procs = list.result?.procs || []
  const connections = list.result?.connections || spec?.known_connections || []

  // Имя занято? Проверяем на странице, а не только на сервере: сервер откажет
  // при записи, но человек к тому моменту уже напишет тело функции.
  const taken = useMemo(
    () =>
      creating &&
      spec?.dag_id &&
      procs.some((p) => p.dag_id === spec.dag_id),
    [creating, spec?.dag_id, procs],
  )

  return (
    <Row gutter={16}>
      <Col xs={24} md={8} lg={6}>
        <Card
          size="small"
          title="Процедуры"
          extra={
            <Button size="small" type="primary" onClick={create}>
              + Создать
            </Button>
          }
        >
          <ActionError error={list.error} onClose={list.reset} />
          <List
            size="small"
            loading={list.loading}
            dataSource={procs}
            locale={{ emptyText: 'процедур пока нет' }}
            renderItem={(item) => (
              <List.Item
                onClick={() => open(item.dag_id)}
                style={{
                  cursor: 'pointer',
                  background:
                    !creating && selected === item.dag_id ? '#e6f4ff' : undefined,
                }}
              >
                <List.Item.Meta
                  title={
                    <Space size={4}>
                      <span>{item.dag_id}</span>
                      {!item.generated && (
                        <Tag color="default" style={{ marginInlineEnd: 0 }}>
                          руками
                        </Tag>
                      )}
                    </Space>
                  }
                  description={
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {humanSchedule(item.schedule)}
                    </Text>
                  }
                />
              </List.Item>
            )}
          />
        </Card>
      </Col>

      <Col xs={24} md={16} lg={18}>
        <ActionError error={load.error} onClose={load.reset} />
        <ActionError error={blank.error} onClose={blank.reset} />

        {alien && <AlienProc proc={alien} />}

        {!alien && !spec && (
          <Card size="small">
            <Paragraph style={{ marginBottom: 0 }}>
              Выберите процедуру слева или создайте новую. Конструктор пишет
              обвязку — расписание, теги, ретраи, соединения, — а тело функции
              остаётся вашим: он читает его в редактор и кладёт обратно
              дословно.
            </Paragraph>
          </Card>
        )}

        {spec && (
          <Space direction="vertical" size="middle" style={{ display: 'flex' }}>
            <Card size="small" title={creating ? 'Новая процедура' : spec.dag_id}>
              <Form layout="vertical" size="small">
                <Row gutter={12}>
                  <Col xs={24} md={12}>
                    <Form.Item
                      label="dag_id"
                      required
                      validateStatus={taken ? 'error' : undefined}
                      help={
                        taken
                          ? 'такой даг уже есть — выберите другое имя'
                          : 'он же имя файла: dags/<dag_id>.py'
                      }
                    >
                      <Input
                        value={spec.dag_id}
                        disabled={!creating}
                        onChange={(e) => set({ dag_id: e.target.value })}
                        placeholder="A56ProceduresПример"
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={24} md={12}>
                    <Form.Item
                      label="task_id"
                      help="имя задачи внутри дага — по нему её видно в airflow"
                    >
                      <Input
                        value={spec.task_id}
                        onChange={(e) => set({ task_id: e.target.value })}
                      />
                    </Form.Item>
                  </Col>
                </Row>

                <Form.Item
                  label="Расписание"
                  help={
                    <>
                      выражение python: dt.timedelta(...), строка-cron или None.{' '}
                      <b>{humanSchedule(spec.schedule)}</b>
                      {/^['"]/.test((spec.schedule || '').trim()) && (
                        <> — cron идёт по часовому поясу airflow, а не по местному</>
                      )}
                    </>
                  }
                >
                  <Select
                    value={spec.schedule}
                    onChange={(v) => set({ schedule: v })}
                    options={SCHEDULE_HINTS.map((s) => ({
                      value: s.value,
                      label: `${s.value}  —  ${s.label}`,
                    }))}
                    showSearch
                    /* Готовые варианты — подсказка, а не ограничение: cron
                       бывает любой, и запрещать его набор значило бы отправлять
                       человека править файл руками, то есть ровно туда, откуда
                       эта страница его и уводит. */
                    filterOption={false}
                    onSearch={(v) => set({ schedule: v })}
                    notFoundContent={null}
                  />
                </Form.Item>

                <Row gutter={12}>
                  <Col xs={12} md={6}>
                    <Form.Item label="Ретраи">
                      <InputNumber
                        min={0}
                        value={spec.retries}
                        onChange={(v) => set({ retries: v ?? 0 })}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} md={6}>
                    <Form.Item label="Пауза, мин">
                      <InputNumber
                        min={0}
                        value={spec.retry_delay_min}
                        onChange={(v) => set({ retry_delay_min: v ?? 0 })}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} md={6}>
                    <Form.Item
                      label="Параллельно"
                      help="max_active_runs"
                    >
                      <InputNumber
                        min={1}
                        value={spec.max_active_runs}
                        onChange={(v) => set({ max_active_runs: v ?? 1 })}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} md={6}>
                    <Form.Item label="Теги">
                      <Select
                        mode="tags"
                        value={spec.tags}
                        onChange={(v) => set({ tags: v })}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                </Row>

                <Form.Item
                  label="Соединения"
                  help="что импортировать из Connect — доступно внутри функции по имени"
                >
                  <Checkbox.Group
                    value={spec.connections}
                    onChange={(v) => set({ connections: v })}
                    options={connections.map((c) => ({ label: c, value: c }))}
                  />
                </Form.Item>

                <Form.Item label="Зачем этот даг" help="одна строка — попадёт в заголовок docstring">
                  <Input
                    value={spec.doc}
                    onChange={(e) => set({ doc: e.target.value })}
                    placeholder="пересчёт ФЕРЗЛ раз в десять минут"
                  />
                </Form.Item>

                <Form.Item
                  label="Заметка"
                  help="свободный текст; конструктор его сохраняет при перезаписи"
                >
                  <Input.TextArea
                    rows={2}
                    value={spec.note}
                    onChange={(e) => set({ note: e.target.value })}
                  />
                </Form.Item>
              </Form>
            </Card>

            <Card
              size="small"
              title={`Тело ${'do_etl_procedures'}() — ваш код`}
              extra={
                <Text type="secondary" style={{ fontSize: 12 }}>
                  Tab — отступ, Shift+Tab — назад
                </Text>
              }
            >
              <Alert
                type="info"
                showIcon
                style={{ marginBottom: 8 }}
                message="Отступ внутри функции конструктор ставит сам"
                description={
                  <>
                    Пишите как в обычном файле, с нулевого уровня:{' '}
                    <Text code>con = DbConnectPost()</Text>. Доступны{' '}
                    <Text code>logging</Text>, <Text code>dt</Text> и отмеченные
                    выше соединения.
                  </>
                }
              />
              <CodeArea
                lang="python"
                minRows={12}
                maxRows={40}
                value={spec.body}
                onChange={(v) => set({ body: v })}
              />
            </Card>

            <Card size="small">
              <Space>
                <Button
                  type="primary"
                  loading={preview.loading}
                  disabled={!spec.dag_id || taken}
                  onClick={() => preview.run(spec)}
                >
                  Предпросмотр
                </Button>
                <Text type="secondary">
                  На диск — только кнопкой «Записать» в предпросмотре.
                </Text>
              </Space>
              <ActionError error={preview.error} onClose={preview.reset} />
            </Card>

            {preview.result && (
              <FilesPreview
                files={preview.result.files}
                unchanged={preview.result.unchanged}
                created={preview.result.created}
                busy={write.loading}
                error={write.error}
                written={write.result}
                onWrite={(chosen) =>
                  write
                    .run({
                      files: chosen.map((f) => [f.path, f.content]),
                      // У НОВОЙ процедуры перезапись запрещена: файл с таким
                      // именем уже есть — значит имя занято, и лучше отказ,
                      // чем молча съеденный чужой даг.
                      overwrite: !creating,
                    })
                    .then((r) => {
                      if (!r) return
                      setReloadToken((n) => n + 1)
                      if (creating) {
                        setCreating(false)
                        setSelected(spec.dag_id)
                      }
                    })
                }
              />
            )}
          </Space>
        )}
      </Col>
    </Row>
  )
}

function AlienProc({ proc }) {
  return (
    <Space direction="vertical" size="middle" style={{ display: 'flex' }}>
      <Alert
        type="warning"
        showIcon
        message={`${proc.dag_id} написан руками — форма его не правит`}
        description={
          <>
            В файле нет маркера конструктора, значит там может быть что угодно:{' '}
            <Text code>A56ProceduresFIX_DEND</Text> — это двести строк логики с
            курсорами по двум базам. Перезапись «тем, что знает форма», стёрла бы
            остальное, поэтому файл показан целиком и только для чтения. Правьте
            его в редакторе на диске: <Text code>{proc.path}</Text>.
          </>
        }
      />
      <Card size="small" title="Обвязка (только чтение)">
        <Space size={[8, 8]} wrap>
          <Tag>расписание: {humanSchedule(proc.schedule)}</Tag>
          <Tag>задача: {proc.task_id || '—'}</Tag>
          <Tag>ретраи: {proc.retries}</Tag>
          <Tag>пауза: {proc.retry_delay_min} мин</Tag>
          <Tag>параллельно: {proc.max_active_runs}</Tag>
          {(proc.tags || []).map((t) => (
            <Tag color="blue" key={t}>
              {t}
            </Tag>
          ))}
          {(proc.connections || []).map((c) => (
            <Tag color="geekblue" key={c}>
              {c}
            </Tag>
          ))}
        </Space>
      </Card>
      <Card size="small" title={proc.path}>
        <CodeArea lang="python" readOnly value={proc.source} minRows={20} maxRows={60} />
      </Card>
    </Space>
  )
}
