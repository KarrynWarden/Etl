import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Form,
  Input,
  InputNumber,
  Popconfirm,
  Radio,
  Row,
  Select,
  Space,
  Spin,
  Switch,
  Tabs,
  Tag,
  Typography,
} from 'antd'
import { UndoOutlined } from '@ant-design/icons'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import CodeArea from './CodeArea'
import ComboBox from './ComboBox'
import FilesPreview from './FilesPreview'
import MappingEditor from './MappingEditor'
import TableNamePair from './TableNamePair'
import LineActions from './LineActions'

const MODES = [
  { value: 'iud', label: 'iud — точечно по журналу' },
  { value: 'delete_insert', label: 'delete_insert — по журналу, но строка целиком' },
  { value: 'section', label: 'section — только isokaudit=4' },
  { value: 'section_compare', label: 'section_compare — сравнение срезов' },
  { value: 'section_compare_with_iud', label: 'section_compare_with_iud — срезы + журнал' },
  { value: 'query_section', label: 'query_section — группы задаёт свой SQL' },
]

const RETRY = [
  { value: 'frequent', label: 'frequent — частый запуск' },
  { value: 'rare', label: 'rare — редкий запуск' },
]

const colName = (c) => c?.column_name || c?.COLUMN_NAME || ''

export default function LineForm({ lineKey, onChanged }) {
  const [spec, setSpec] = useState(null)
  const [original, setOriginal] = useState(null)
  const [placement, setPlacement] = useState(null)

  const load = useAction(api.line)
  const preview = useAction(api.preview)
  const write = useAction(api.write)
  const push = useAction(api.gitPush)
  const groupDags = useAction(api.groupDags)
  const tagsQuery = useAction(api.tags)

  const reload = useCallback(() => {
    if (!lineKey) return
    preview.reset()
    write.reset()
    load.run(lineKey).then((data) => {
      if (!data) return
      setSpec(data.spec)
      setOriginal(JSON.stringify(data.spec))
      setPlacement(data.placement)
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lineKey])

  useEffect(() => {
    reload()
    groupDags.run()
    tagsQuery.run()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lineKey])

  const dirty = useMemo(
    () => Boolean(spec && original && JSON.stringify(spec) !== original),
    [spec, original],
  )
  const saved = useMemo(() => (original ? JSON.parse(original) : {}), [original])

  const masterNames = useMemo(() => (spec?.master_cols || []).map(colName), [spec])
  const slaveNames = useMemo(() => (spec?.slave_cols || []).map(colName), [spec])

  if (!lineKey)
    return (
      <Card>
        <Typography.Text type="secondary">
          Выберите линию слева — или заведите новую на вкладке «Новая линия».
        </Typography.Text>
      </Card>
    )

  if (load.loading && !spec)
    return (
      <Card>
        <Spin /> <Typography.Text type="secondary">Читаю линию…</Typography.Text>
      </Card>
    )

  if (load.error)
    return (
      <Card title={lineKey}>
        <ActionError error={load.error} />
      </Card>
    )

  if (!spec) return null

  const writeChosen = (chosen) =>
    write
      .run({ files: chosen.map((f) => [f.path, f.content]), overwrite: true })
      .then((res) => {
        if (!res) return undefined
        setOriginal(JSON.stringify(spec))
        onChanged?.()
        return res
      })

  const patch = (changes) => setSpec((p) => ({ ...p, ...changes }))
  const patchExtra = (changes) =>
    setSpec((p) => ({ ...p, extra: { ...(p.extra || {}), ...changes } }))
  const extra = spec.extra || {}
  // в конфиге это строка через запятую, в форме — список
  const excludeList = String(extra.auditExcludeFields || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)

  const settings = (
    <Form layout="vertical">
      <TableNamePair
        resetKey={lineKey}
        masterValue={spec.table_master}
        masterDb={spec.db_master}
        onMasterChange={(v) => patch({ table_master: v })}
        slaveValue={spec.table_slave}
        slaveDb={spec.db_slave}
        onSlaveChange={(v) => patch({ table_slave: v })}
      />

      <Row gutter={16}>
        <Col span={8}>
          <Form.Item label="Режим переноса">
            <Select value={spec.mode} onChange={(v) => patch({ mode: v })} options={MODES} />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Колонка периода (ведущая)" help="список — колонки ведущей, можно вписать своё">
            <ComboBox
              value={spec.period_column}
              onChange={(v) => patch({ period_column: v })}
              options={masterNames}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Колонка периода (ведомая)">
            <ComboBox
              value={spec.slave_period_column}
              onChange={(v) => patch({ slave_period_column: v })}
              options={slaveNames}
            />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="Фильтр ведущей (filterClause)" help="приклеивается к источнику: doctype IN (2, 3)">
            <Input
              value={extra.filterClause || ''}
              onChange={(e) => patchExtra({ filterClause: e.target.value || undefined })}
            />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="Фильтр ведомой (filterClauseSlave)" help="ограничивает DELETE и выборку ведомой">
            <Input
              value={extra.filterClauseSlave || ''}
              onChange={(e) => patchExtra({ filterClauseSlave: e.target.value || undefined })}
            />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={8}>
          <Form.Item label="Конфликт: доп. колонки (conflictExtra)">
            <Input
              value={extra.conflictExtra || ''}
              onChange={(e) => patchExtra({ conflictExtra: e.target.value || undefined })}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Конфликт: условие (conflictWhere)" help="для ЧАСТИЧНОГО уникального индекса в Postgres">
            <Input
              value={extra.conflictWhere || ''}
              onChange={(e) => patchExtra({ conflictWhere: e.target.value || undefined })}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item
            label="Не сверять поля в аудите (auditExcludeFields)"
            help="список — колонки ведущей; чего нет в списке, можно вписать"
          >
            {/* Колонки известны — незачем заставлять человека печатать их по
                памяти и через запятую. В конфиг всё равно уходит канон:
                строка через запятую (do_audit._asNameSet принимает и список,
                но менять формат существующих конфигов не за чем). */}
            <Select
              mode="tags"
              style={{ width: '100%' }}
              placeholder="колонки, которые аудит не сверяет"
              value={excludeList}
              onChange={(v) =>
                patchExtra({ auditExcludeFields: v.length ? v.join(', ') : undefined })
              }
              options={masterNames.map((n) => ({ value: n }))}
            />
          </Form.Item>
        </Col>
      </Row>

      <DependenciesEditor
        value={extra.iudDependencies}
        masterNames={masterNames}
        onChange={(v) => patchExtra({ iudDependencies: v && v.length ? v : undefined })}
      />

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label="Период сравнивать по дате, без времени (truncatePeriod)"
            help="нужен, когда стороны хранят период по-разному; колонка при этом уходит под функцию и индекс не работает"
          >
            <Switch
              checked={Boolean(extra.truncatePeriod)}
              onChange={(v) => patchExtra({ truncatePeriod: v || undefined })}
            />
          </Form.Item>
        </Col>
        <Col span={6}>
          {/* Переключателей skipAudit было ДВА — здесь и в «Действиях», и
              работали они по-разному: тот писал файл сразу по щелчку, этот ждал
              предпросмотра. Остался один, и именно этот: ни одна правка не
              должна уходить на диск раньше кнопки «Записать». */}
          <Form.Item
            label="Не аудировать линию (skipAudit)"
            help="переносим, но не сверяем"
          >
            <Switch
              checked={Boolean(extra.skipAudit)}
              onChange={(v) => patchExtra({ skipAudit: v || undefined })}
            />
          </Form.Item>
        </Col>
        <Col span={6}>
          <Form.Item
            label="Отключить линию (disabled)"
            help="даг остаётся, но пропускает её — и аудит тоже"
          >
            <Switch
              checked={Boolean(extra.disabled)}
              onChange={(v) => patchExtra({ disabled: v || undefined })}
            />
          </Form.Item>
        </Col>
      </Row>

      <Form.Item label="Комментарий линии (_doc)">
        <Input.TextArea
          autoSize={{ minRows: 3, maxRows: 12 }}
          value={spec.doc || ''}
          onChange={(e) => patch({ doc: e.target.value })}
        />
      </Form.Item>
    </Form>
  )

  const inGroup = Boolean(spec.group_dag_id)
  const dagTab = (
    <Form layout="vertical">
      <Form.Item label="Где живёт даг линии">
        <Radio.Group
          value={inGroup ? 'group' : 'own'}
          onChange={(e) =>
            patch(
              e.target.value === 'group'
                ? { group_dag_id: groupDags.result?.group_dags?.[0]?.dag_id || '', dag_id: '' }
                : { group_dag_id: '', dag_id: spec.dag_id || '' },
            )
          }
        >
          <Radio.Button value="own">Свой даг на линию</Radio.Button>
          <Radio.Button value="group">В составном даге</Radio.Button>
        </Radio.Group>
      </Form.Item>

      {inGroup ? (
        <Form.Item
          label="Составной даг"
          help="расписание, теги и ретраи у составного дага общие на все его линии — здесь они не меняются"
        >
          <ComboBox
            value={spec.group_dag_id}
            onChange={(v) => patch({ group_dag_id: v })}
            options={(groupDags.result?.group_dags || []).map((g) => ({
              value: g.dag_id,
              label: g.parsed ? g.dag_id : `${g.dag_id} (написан руками)`,
            }))}
          />
        </Form.Item>
      ) : (
        <>
          <Form.Item
            label="dag_id"
            help="это идентификатор задачи в Airflow вместе со всей её историей: смена имени заводит НОВЫЙ даг, а прежний остаётся в списке без расписания"
          >
            {/* Пока имя не трогали, пишем в тот же файл, что и лежит на диске
                (dag_file_rel): имя файла и dag_id совпадают не у всех дагов.
                Как только имя поменяли — привязку снимаем, иначе новый даг
                уехал бы в старый файл. */}
            <Input
              value={spec.dag_id || ''}
              onChange={(e) =>
                patch(
                  e.target.value === saved.dag_id
                    ? { dag_id: e.target.value, dag_file_rel: saved.dag_file_rel }
                    : { dag_id: e.target.value, dag_file_rel: '' },
                )
              }
            />
          </Form.Item>
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="Расписание">
                <Select
                  value={spec.schedule_kind || 'interval'}
                  onChange={(v) => patch({ schedule_kind: v })}
                  options={[
                    { value: 'interval', label: 'каждые N минут' },
                    { value: 'cron', label: 'cron-выражение' },
                  ]}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              {(spec.schedule_kind || 'interval') === 'cron' ? (
                <Form.Item label="cron">
                  <Input
                    placeholder="0 5,7 * * *"
                    value={spec.schedule_cron || ''}
                    onChange={(e) => patch({ schedule_cron: e.target.value })}
                  />
                </Form.Item>
              ) : (
                <Form.Item label="каждые, мин">
                  <InputNumber
                    min={1}
                    style={{ width: '100%' }}
                    value={spec.schedule_minutes || 1}
                    onChange={(v) => patch({ schedule_minutes: v })}
                  />
                </Form.Item>
              )}
            </Col>
            <Col span={8}>
              <Form.Item label="Режим ретраев">
                <Select
                  value={spec.retry_mode || 'frequent'}
                  onChange={(v) => patch({ retry_mode: v })}
                  options={RETRY}
                />
              </Form.Item>
            </Col>
          </Row>
        </>
      )}

      <Form.Item label="Теги дага" help="своё значение вводится как есть">
        <Select
          mode="tags"
          value={spec.tags || []}
          onChange={(v) => patch({ tags: v })}
          options={(tagsQuery.result?.tags || []).map((t) => ({ value: t }))}
        />
      </Form.Item>
    </Form>
  )

  const sqlTab = (
    <Form layout="vertical">
      <Form.Item
        label={`SELECT ведущей${spec.select_sql ? ` — ${spec.select_sql}` : ' (нет, читается таблица целиком)'}`}
        help="если задан, имена в конфиге — это ПСЕВДОНИМЫ запроса, а не колонки ведущей"
      >
        <CodeArea
          minRows={6}
          maxRows={24}
          value={spec.select_sql_text || ''}
          onChange={(e) => patch({ select_sql_text: e.target.value })}
        />
      </Form.Item>
      {/* SQL периодов читает РОВНО ОДИН режим — query_section (do_etl.
          _runQuerySection); остальные берут группы из журнала или сравнением
          срезов. Пустое поле у линии, которая его не использует, выглядело как
          недоделанная настройка: конструктор такой текст всё равно не запишет
          и ключ periodsSql не создаст. */}
      {spec.mode === 'query_section' ? (
        <Form.Item
          label={`SQL периодов${spec.periods_sql ? ` — ${spec.periods_sql}` : ''}`}
          help="обязателен для query_section: отсюда берётся список групп для перезаливки"
        >
          <CodeArea
            minRows={4}
            maxRows={16}
            value={spec.periods_sql_text || ''}
            onChange={(e) => patch({ periods_sql_text: e.target.value })}
          />
        </Form.Item>
      ) : (
        spec.periods_sql_text && (
          <Alert
            type="warning"
            showIcon
            message="У линии сохранён SQL периодов, но режим его не читает"
            description={`Он нужен только режиму query_section, а здесь ${spec.mode}. Текст остаётся в ${spec.periods_sql || 'файле'}, но в конфиг ключ periodsSql не попадёт.`}
          />
        )
      )}
    </Form>
  )

  return (
    <Space direction="vertical" size="middle" style={{ display: 'flex' }}>
      <Card
        title={
          <Space wrap>
            <b>{lineKey}</b>
            <Tag>{spec.db_master} → {spec.db_slave}</Tag>
            {placement?.group_dag ? (
              <Tag color="blue">составной: {placement.group_dag}</Tag>
            ) : (
              <Tag>свой даг</Tag>
            )}
            {dirty && <Tag color="orange">есть несохранённые правки</Tag>}
          </Space>
        }
        extra={
          <Space>
            <Popconfirm
              title="Отменить все правки?"
              description="Форма вернётся к тому, что сейчас лежит на диске."
              okText="Отменить правки"
              cancelText="Нет"
              disabled={!dirty}
              onConfirm={reload}
            >
              <Button icon={<UndoOutlined />} disabled={!dirty}>
                Отменить
              </Button>
            </Popconfirm>
            <Button
              type="primary"
              loading={preview.loading}
              onClick={() => {
                write.reset()
                preview.run(spec)
              }}
            >
              Предпросмотр
            </Button>
          </Space>
        }
      >
        <Tabs
          items={[
            { key: 'set', label: 'Настройки', children: settings },
            {
              key: 'map',
              label: `Колонки (${(spec.pairs || []).length})`,
              children: <MappingEditor spec={spec} onChange={patch} />,
            },
            { key: 'dag', label: 'Даг и расписание', children: dagTab },
            { key: 'sql', label: 'SQL', children: sqlTab },
            {
              key: 'act',
              label: 'Действия',
              children: (
                <LineActions
                  lineKey={lineKey}
                  spec={spec}
                  // Переименование отдаёт НОВЫЙ ключ: старого больше нет, и
                  // reload() по нему уткнулся бы в «конфиг не найден». Пробрасываем
                  // ключ наверх — там переключат выбор, а форма пересоберётся.
                  onChanged={(newKey) => {
                    if (newKey) onChanged?.(newKey)
                    else {
                      reload()
                      onChanged?.()
                    }
                  }}
                />
              ),
            },
          ]}
        />
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
          onWrite={writeChosen}
          pushing={push.loading}
          pushError={push.error}
          pushed={push.result}
          // «Записать и запушить» — одно действие: запись, и только если она
          // прошла, коммит. Порядок важен: пушить неудавшуюся запись нечего.
          onPush={(chosen) =>
            writeChosen(chosen).then(
              (res) =>
                res && push.run({ message: `конструктор: ${lineKey}` }).then(() => onChanged?.()),
            )
          }
        />
      )}

      {!preview.result && dirty && (
        <Alert
          type="info"
          showIcon
          message="Правки пока только в форме"
          description="На диск они попадут после «Предпросмотр» → «Записать». Кнопка «Отменить» вернёт всё как было."
        />
      )}
    </Space>
  )
}

// Зависимости линии: чужая таблица, чья правка вводит строку в выборку.
//
// Нужны там, где строка появляется из-за соседней таблицы, а сама ведущая не
// меняется. Так у reqprepmomocheck: REQPREPSMO заводится с пустым DIRDT, к ней
// привязывается REQPREPMO, и лишь потом DIRDT проставляют — на этом шаге
// ведущую не трогают вовсе, и её триггер молчит совершенно по делу.
//
// Лечится это не умным триггером, а лишней МЕТКОЙ в журнале: триггер соседней
// таблицы пишет строку со своим ключом под чужим именем, а разложить её в ключи
// ведущей умеет перенос. Условие запроса вместе с JOIN'ами в триггер при этом
// не переезжает — он остаётся ровно таким же глупым.
function DependenciesEditor({ value, masterNames, onChange }) {
  const items = Array.isArray(value) ? value : []
  const patch = (i, changes) =>
    onChange(items.map((d, n) => (n === i ? { ...d, ...changes } : d)))

  return (
    <Card
      size="small"
      style={{ marginBottom: 16 }}
      title="Зависимости: правка в соседней таблице вводит строку в выборку"
      extra={
        <Button
          size="small"
          onClick={() =>
            onChange([
              ...items,
              { tablename: '', column: '', sourceTable: '', sourceDb: 'Post',
                sourcePeriodColumn: 'createdate' },
            ])
          }
        >
          + Добавить
        </Button>
      }
    >
      {items.length === 0 && (
        <Typography.Text type="secondary">
          Нужны, только когда строка появляется в выборке из-за правки в другой
          таблице, а сама ведущая при этом не меняется — её триггер тогда молчит
          по делу, и события линия не получает.
        </Typography.Text>
      )}

      {items.map((dep, i) => (
        <Row gutter={12} key={i} align="bottom" style={{ marginBottom: 8 }}>
          <Col span={5}>
            <Form.Item
              label="Метка в журнале"
              help="tablename в etl_log_iud_row"
              style={{ marginBottom: 0 }}
            >
              <Input
                value={dep.tablename || ''}
                onChange={(e) => patch(i, { tablename: e.target.value })}
              />
            </Form.Item>
          </Col>
          <Col span={6}>
            <Form.Item
              label="Колонка ведущей"
              help="по ней ключ чужой строки раскладывается в ключи ведущей"
              style={{ marginBottom: 0 }}
            >
              {/* Колонка обязана отдаваться ЗАПРОСОМ линии — раскладывать
                  можно только то, что запрос возвращает. В структуру она при
                  этом входить не обязана: перенос выбирает из запроса лишь то,
                  что перечислено в структуре. */}
              <ComboBox
                value={dep.column || ''}
                onChange={(v) => patch(i, { column: v })}
                options={masterNames}
              />
            </Form.Item>
          </Col>
          <Col span={6}>
            <Form.Item
              label="Таблица-источник"
              help="на неё встанет метка; её триггер соберёт конструктор"
              style={{ marginBottom: 0 }}
            >
              <Input
                value={dep.sourceTable || ''}
                onChange={(e) => patch(i, { sourceTable: e.target.value })}
              />
            </Form.Item>
          </Col>
          <Col span={3}>
            <Form.Item label="БД" style={{ marginBottom: 0 }}>
              <Select
                value={dep.sourceDb || 'Post'}
                onChange={(v) => patch(i, { sourceDb: v })}
                options={[{ value: 'Post' }, { value: 'Orcl' }]}
              />
            </Form.Item>
          </Col>
          <Col span={3}>
            <Form.Item label="Период источника" style={{ marginBottom: 0 }}>
              <Input
                value={dep.sourcePeriodColumn || ''}
                onChange={(e) => patch(i, { sourcePeriodColumn: e.target.value })}
              />
            </Form.Item>
          </Col>
          <Col span={1}>
            <Button
              size="small"
              danger
              onClick={() => onChange(items.filter((_, n) => n !== i))}
            >
              ×
            </Button>
          </Col>
        </Row>
      ))}

      {items.length > 0 && (
        <Alert
          type="info"
          showIcon
          style={{ marginTop: 8 }}
          message="Период источника в перенос не идёт"
          description={
            <>
              Он нужен только триггеру источника, чтобы было что положить в
              колонку <Typography.Text code>period</Typography.Text> журнала.
              Период переносимых строк линия берёт из СВОЕЙ выборки: чужой
              период ушёл бы в <Typography.Text code>etl_jobs</Typography.Text>,
              и линия отчиталась бы о переносе не тех групп, которые перенесла.
            </>
          }
        />
      )}
    </Card>
  )
}
