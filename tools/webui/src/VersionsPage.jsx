import { useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Input,
  Modal,
  Popconfirm,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd'
import { CloudUploadOutlined, ReloadOutlined, UndoOutlined } from '@ant-design/icons'

import { api } from './api'
import { useAction } from './useAction'
import ActionError from './ActionError'
import FilesPreview from './FilesPreview'

// Версии и выкладка.
//
// Две среды, и путать их дорого (deploy/README-prod.md):
//
//   gitea ── ветка test ──► /opt/airflow-test/etl.git ──► airflow-test (:8082)
//         └─ ветка prod ──► /opt/airflow-prod/etl.git ──► прод (:8080)
//
// Конструктор живёт в клоне на ТЕСТЕ и пушит в свою ветку. Прод — не «хвост»
// теста, а собранное состояние: каждая выкладка это коммит поверх предыдущего
// прода с деревом теста, помеченный тегом prod-ГГГГММДД-ЧЧММ. Поэтому
// недоделанная работа на тесте на прод не уезжает сама.
//
// Откат — и теста, и прода — всегда НОВЫЙ КОММИТ ПОВЕРХ, никогда не reset с
// force-push: у обеих веток есть клоны (dev-ПК, test-src, prod-src, сам
// конструктор), и переписанную историю пришлось бы чинить руками на каждом.
const дата = (iso) => {
  if (!iso) return ''
  const d = new Date(iso)
  return Number.isNaN(d.getTime())
    ? iso
    : d.toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })
}

export default function VersionsPage() {
  const [picked, setPicked] = useState(null)     // версия, выбранная в таблице
  const [plan, setPlan] = useState(null)         // разобранный план отката
  // Выкладка спрашивает слово. Не защита — задержка: API слушает localhost,
  // наружу его отдаёт Apache со своей авторизацией, так что от чужого слово не
  // спасает и не для того заведено. Оно для своего: «выложить на прод» —
  // единственная кнопка, чьё последствие нельзя посмотреть в предпросмотре и
  // отменить соседней кнопкой, и набрать слово руками значит на секунду
  // остановиться и перечитать, что уезжает. Проверяет сервер (git_ops), здесь
  // только спрашиваем.
  const [ask, setAsk] = useState(null)           // {from_ref, title} или null
  const [word, setWord] = useState('')
  // Во время отладки за день набегают десятки коммитов, а прод-точек среди них
  // единицы. Искать их глазами в общем списке — то же, что искать линию без
  // фильтра по имени: работает, пока список короткий.
  const [onlyProd, setOnlyProd] = useState(false)

  const versions = useAction(api.gitVersions)
  const prodStatus = useAction(api.prodStatus)
  const prodDiff = useAction(api.prodDiff)
  const prodDeploy = useAction(api.prodDeploy)
  const rollbackPlan = useAction(api.rollbackPlan)
  const rollbackApply = useAction(api.rollbackApply)

  useEffect(() => {
    versions.run({ limit: 40 })
    prodStatus.run({ probe: false })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const reload = (refreshTags = false) => {
    setPlan(null)
    setPicked(null)
    // Теги выкладок ставит тот клон, из которого запускали deploy-prod.sh, и
    // пушит в прод-репозиторий. У конструктора их может не быть вовсе —
    // тогда список выкладок пуст, и выглядит это как «функции нет».
    versions.run({ limit: 40, refresh_tags: refreshTags || undefined })
  }

  const st = prodStatus.result
  // Выкладка и откат НЕ ждут проверки доступа. Раньше обе кнопки были заперты,
  // пока «Проверить доступ» не сходит по сети, — то есть обычное действие
  // начиналось с обязательного лишнего шага ради случая, которого практически
  // не бывает. Недоступный прод и так виден: скрипт вернёт ненулевой код, а его
  // вывод показан целиком. Отказ по факту лучше запрета заранее: запрет ничего
  // не объясняет, а неудача объясняет всё.
  const all = versions.result?.versions || []
  const rows = onlyProd
    ? all.filter((r) => (r.tags || []).some((t) => t.startsWith('prod-')))
    : all
  const prodList = versions.result?.prod_tags || []
  const prodCount = all.filter((r) =>
    (r.tags || []).some((t) => t.startsWith('prod-'))).length

  const showPlan = (ref) => {
    setPicked(ref)
    setPlan(null)
    rollbackPlan.run({ ref }).then((r) => r && setPlan(r))
  }

  return (
    <Space direction="vertical" size="middle" style={{ display: 'flex' }}>
      {/* ── ПРОД ───────────────────────────────────────────────────────── */}
      <Card
        size="small"
        title={
          <Space>
            <b>Прод</b>
            {st?.prod_head && <Tag>{st.prod_head}</Tag>}
            {st?.reachable === true && <Tag color="green">доступен</Tag>}
            {st?.reachable === false && <Tag color="red">недоступен</Tag>}
          </Space>
        }
        extra={
          <Space>
            <Tooltip title="Проверить, может ли конструктор достучаться до прод-репозитория (ходит по той же дороге, что и push)">
              <Button
                size="small"
                icon={<ReloadOutlined />}
                loading={prodStatus.loading}
                onClick={() => prodStatus.run({ probe: true })}
              >
                Проверить доступ
              </Button>
            </Tooltip>
            <Button
              size="small"
              loading={prodDiff.loading}
              onClick={() => prodDiff.run({})}
            >
              Чем прод отличается от теста
            </Button>
            {/* Кнопка доступна всегда. Прав у конструктора может и не быть
                (прод-репо в группе etlprod, сервис под jupyter из etldev) — но
                узнаётся это по отказу выкладки, с полным выводом скрипта и
                командами для рук. Запрет заранее не объясняет ничего, а
                неудача объясняет всё. */}
            <Tooltip title="Соберёт коммит на прод из текущего теста и запушит">
              <Button
                type="primary"
                danger
                icon={<CloudUploadOutlined />}
                loading={prodDeploy.loading}
                onClick={() => {
                  setWord('')
                  setAsk({ from_ref: null, title: 'Выложить тест на ПРОД' })
                }}
              >
                Выложить на прод
              </Button>
            </Tooltip>
          </Space>
        }
      >
        <Space direction="vertical" size="small" style={{ display: 'flex' }}>
          <Typography.Text type="secondary">
            Ветка конструктора: <Typography.Text code>{versions.result?.branch || '—'}</Typography.Text>
            {st?.remote && (
              <> · прод-репозиторий: <Typography.Text code>{st.remote}</Typography.Text></>
            )}
          </Typography.Text>

          {st?.detail && (
            <Alert
              type={st.reachable === false || !st.remote ? 'warning' : 'info'}
              showIcon
              // Не «выложить не выйдет»: выложить теперь можно попробовать
              // всегда, и отказ по факту скажет больше, чем запрет заранее.
              message={st.reachable === false ? 'Прод не ответил на проверку' : 'Состояние прода'}
              description={<pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{st.detail}</pre>}
            />
          )}
          <ActionError error={prodStatus.error || prodDiff.error || prodDeploy.error} />
          {prodDiff.result && (
            <pre style={{ maxHeight: '30vh', overflow: 'auto', margin: 0 }}>
              {prodDiff.result.log}
            </pre>
          )}
          {prodDeploy.result && (
            <Alert
              // Скрипт выходит с НУЛЁМ и когда выкладывать нечего: прод уже в
              // нужном состоянии, это не ошибка. Но «Выложено» здесь — ложь:
              // ни коммита, ни тега не появилось, и человек несколько раз
              // подряд видел успех, не понимая, почему на проде пусто.
              type={
                !prodDeploy.result.ok
                  ? 'error'
                  : prodDeploy.result.deployed
                    ? 'success'
                    : 'warning'
              }
              showIcon
              message={
                !prodDeploy.result.ok
                  ? 'Выкладка не прошла'
                  : prodDeploy.result.deployed
                    ? `Выложено: ${prodDeploy.result.tag || 'коммит создан'}`
                    : 'Ничего не выложено — прод уже в этом состоянии'
              }
              description={
                <>
                  <pre style={{ maxHeight: '40vh', overflow: 'auto', margin: 0 }}>
                    {prodDeploy.result.log}
                  </pre>
                  {/* Команды для рук — здесь, а не постоянным баннером. Нужны
                      они ровно в этот момент: у конструктора нет прав на
                      прод-репо (он под jupyter из etldev, репо в etlprod), и
                      тогда выложить может только тот, у кого права есть. */}
                  {prodDeploy.result.ok && !prodDeploy.result.deployed && (
                    <Typography.Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
                      Новый коммит на проде не появился, тега тоже нет. Обычно
                      это значит, что источник выкладки (<Typography.Text code>origin/test</Typography.Text>)
                      не содержит новых правок: их записали, но не запушили.
                      Проверьте в шапке «не запушено файлов».
                    </Typography.Paragraph>
                  )}
                  {!prodDeploy.result.ok && (
                    <details style={{ marginTop: 8 }}>
                      <summary>выложить руками — тому, у кого есть права</summary>
                      <pre style={{ margin: '6px 0 0', whiteSpace: 'pre-wrap' }}>
{`ssh devel@airflow
cd /путь/к/клону
bash deploy/deploy-prod.sh --diff     # сначала посмотреть, что уедет
bash deploy/deploy-prod.sh            # выложить`}
                      </pre>
                    </details>
                  )}
                </>
              }
            />
          )}
        </Space>
      </Card>

      {/* ── ВЫКЛАДКИ ПРОДА ─────────────────────────────────────────────── */}
      {/* Карточка показывается ВСЕГДА, даже пустой. Раньше при пустом списке её
          не было вовсе, и «откатить прод к прошлой версии» выглядело как
          отсутствующая возможность — хотя дело в том, что тегов нет в клоне:
          их ставит тот клон, из которого запускали deploy-prod.sh. */}
      <Card
        size="small"
        title="Выкладки прода"
        extra={
          <Tooltip title="Забрать теги prod-* из прод-репозитория: их ставит тот клон, из которого запускали выкладку">
            <Button
              size="small"
              icon={<ReloadOutlined />}
              loading={versions.loading}
              onClick={() => reload(true)}
            >
              Обновить из прода
            </Button>
          </Tooltip>
        }
      >
        {versions.result?.tags_note && (
          <Alert
            type="warning"
            showIcon
            style={{ marginBottom: 8 }}
            message="Теги из прода забрать не вышло — показаны те, что есть локально"
            description={<pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{versions.result.tags_note}</pre>}
          />
        )}
        {prodList.length === 0 ? (
          <Typography.Text type="secondary">
            Выкладок не видно. Если они точно были — нажмите «Обновить из прода»:
            теги ставит тот клон, из которого запускали{' '}
            <Typography.Text code>deploy/deploy-prod.sh</Typography.Text>, и в этом
            клоне их может не быть.
          </Typography.Text>
        ) : (
          <Table
            size="small"
            bordered
            pagination={false}
            scroll={{ y: 220 }}
            rowKey="tag"
            dataSource={prodList}
            columns={[
              { title: 'Когда', dataIndex: 'date', width: 150, render: дата },
              { title: 'Версия', dataIndex: 'tag', width: 200,
                render: (v) => <Typography.Text code>{v}</Typography.Text> },
              { title: 'Что выкладывали', dataIndex: 'subject' },
              {
                title: '',
                width: 200,
                render: (_v, r) => (
                  <Button
                    size="small"
                    danger
                    icon={<UndoOutlined />}
                    onClick={() => {
                      setWord('')
                      setAsk({ from_ref: r.tag, title: `Вернуть прод к ${r.tag}` })
                    }}
                  >
                    откатить прод сюда
                  </Button>
                ),
              },
            ]}
          />
        )}
      </Card>

      {/* ── ВЕРСИИ ТЕСТА ───────────────────────────────────────────────── */}
      <Card
        size="small"
        title={<b>Версии {versions.result?.branch ? `(${versions.result.branch})` : ''}</b>}
        extra={
          <Space>
            {/* Десятки коммитов за день отладки, прод-точек среди них единицы.
                Фильтр — то же, что фильтр по имени в списке линий: без него
                поиск работает, только пока список короткий. */}
            <Checkbox
              checked={onlyProd}
              disabled={!prodCount}
              onChange={(e) => setOnlyProd(e.target.checked)}
            >
              только выложенные на прод{prodCount ? ` (${prodCount})` : ''}
            </Checkbox>
            <Tooltip title="Перечитать историю">
              <Button
                size="small"
                icon={<ReloadOutlined />}
                loading={versions.loading}
                onClick={() => reload(false)}
              />
            </Tooltip>
          </Space>
        }
      >
        <Typography.Paragraph type="secondary" style={{ marginTop: 0 }}>
          Показаны версии, в которых менялось хозяйство конструктора —
          <Typography.Text code>etlFolder/</Typography.Text> и
          <Typography.Text code>dags/</Typography.Text>. Откат возвращает
          состояние выбранной версии <b>новым коммитом поверх</b>: история не
          переписывается, ничей клон не ломается. Код (<Typography.Text code>Functions/</Typography.Text>,
          <Typography.Text code>tools/</Typography.Text>) отсюда не откатывается —
          конструктор запущен из этого же дерева.
        </Typography.Paragraph>

        <ActionError error={versions.error || rollbackPlan.error} />

        <Table
          size="small"
          bordered
          pagination={false}
          scroll={{ y: 360 }}
          rowKey="sha"
          dataSource={rows}
          rowClassName={(r) => (r.sha === picked ? 'ant-table-row-selected' : '')}
          columns={[
            { title: 'Когда', dataIndex: 'date', width: 150, render: дата },
            { title: 'Версия', dataIndex: 'short', width: 110,
              render: (v) => <Typography.Text code>{v}</Typography.Text> },
            { title: 'Кто', dataIndex: 'author', width: 140 },
            {
              title: 'Что менялось',
              render: (_v, r) => (
                <Space size={4} wrap>
                  <span>{r.subject}</span>
                  {r.tags.map((t) =>
                    t.startsWith('prod-') ? (
                      // Прод-точку надо видеть, не вчитываясь: именно её ищут,
                      // когда спрашивают «до какой версии откатить».
                      <Tag key={t} color="green" icon={<CloudUploadOutlined />}>
                        {t}
                      </Tag>
                    ) : (
                      <Tag key={t}>{t}</Tag>
                    ),
                  )}
                </Space>
              ),
            },
            {
              title: '',
              width: 150,
              render: (_v, r, i) => (
                <Button
                  size="small"
                  disabled={i === 0}
                  loading={rollbackPlan.loading && picked === r.sha}
                  onClick={() => showPlan(r.sha)}
                >
                  {i === 0 ? 'текущая' : 'откатить сюда'}
                </Button>
              ),
            },
          ]}
        />
      </Card>

      {/* ── СЛОВО-ПОДТВЕРЖДЕНИЕ ────────────────────────────────────────── */}
      <Modal
        open={Boolean(ask)}
        title={ask?.title}
        okText="Выложить"
        okButtonProps={{ danger: true, disabled: !word.trim() }}
        cancelText="Отмена"
        confirmLoading={prodDeploy.loading}
        onCancel={() => setAsk(null)}
        onOk={() => prodDeploy
            .run({ from_ref: ask.from_ref || undefined, password: word })
            .then((r) => {
              if (!r) return
              setAsk(null)
              prodStatus.run({ probe: true })
              // Тег только что поставлен в этом же клоне — но список версий
              // держит прежний ответ. Перечитываем, иначе новой выкладки в
              // «Выкладках прода» не видно до перезагрузки страницы.
              reload(false)
            })}
      >
        <Typography.Paragraph>
          {ask?.from_ref ? (
            <>
              Дерево выкладки <Typography.Text code>{ask.from_ref}</Typography.Text> уедет
              на прод новым коммитом поверх текущего — без переписывания истории.
            </>
          ) : (
            <>
              Уедет всё состояние теста. Перед push пройдёт гейт (check-dags.sh)
              по тому самому дереву; выкладка получит тег prod-ГГГГММДД-ЧЧММ,
              откатить её можно будет выкладкой предыдущего тега.
            </>
          )}
        </Typography.Paragraph>
        <Typography.Paragraph type="secondary">
          Это единственное действие в конструкторе, которое нельзя посмотреть в
          предпросмотре и отменить соседней кнопкой. Наберите слово-подтверждение.
        </Typography.Paragraph>
        <Input.Password
          autoFocus
          value={word}
          placeholder="слово-подтверждение"
          onChange={(e) => setWord(e.target.value)}
          onPressEnter={() => word.trim() && prodDeploy
            .run({ from_ref: ask.from_ref || undefined, password: word })
            .then((r) => {
              if (!r) return
              setAsk(null)
              prodStatus.run({ probe: true })
              // Тег только что поставлен в этом же клоне — но список версий
              // держит прежний ответ. Перечитываем, иначе новой выкладки в
              // «Выкладках прода» не видно до перезагрузки страницы.
              reload(false)
            })}
        />
        <ActionError error={prodDeploy.error} />
      </Modal>

      {/* ── ПЛАН ОТКАТА ────────────────────────────────────────────────── */}
      {plan && (
        <Card
          size="small"
          title={
            <Space wrap>
              <b>Откат до {plan.resolved?.slice(0, 9)}</b>
              <Typography.Text type="secondary">{plan.subject}</Typography.Text>
              <Tag>{дата(plan.date)}</Tag>
            </Space>
          }
          extra={<Button size="small" onClick={() => setPlan(null)}>Закрыть</Button>}
        >
          <Space direction="vertical" size="small" style={{ display: 'flex' }}>
            {plan.dirty ? (
              <Alert
                type="error"
                showIcon
                message="Есть несохранённые правки — откат поверх них запрещён"
                description={
                  <>
                    Потом не разберёшь, что вернулось из версии, а что осталось
                    от недоделанного. Сначала запушьте их или отмените.
                    <pre style={{ margin: '8px 0 0', whiteSpace: 'pre-wrap' }}>{plan.dirty}</pre>
                  </>
                }
              />
            ) : !plan.files.length && !plan.remove.length ? (
              <Alert type="info" showIcon message="Откатывать нечего — рабочая копия уже в этом состоянии" />
            ) : (
              <>
                {plan.remove.length > 0 && (
                  <Alert
                    type="warning"
                    showIcon
                    message={`Будет удалено файлов: ${plan.remove.length} (в той версии их ещё не было)`}
                    description={
                      <ul style={{ margin: 0, paddingLeft: 18 }}>
                        {plan.remove.map((p) => <li key={p}><Typography.Text code>{p}</Typography.Text></li>)}
                      </ul>
                    }
                  />
                )}
                {/* Тот же предпросмотр с диффом, что у обычной правки: откат —
                    это такая же запись файлов, и выглядеть она должна так же.
                    Кнопка записи здесь своя: откат обязан пройти целиком
                    (вместе с удалениями), выборочно откатывать нечего. */}
                <FilesPreview files={plan.files} unchanged={plan.unchanged} created={plan.created} readOnly />
                <Popconfirm
                  title="Откатить и запушить?"
                  description="Состояние вернётся новым коммитом поверх текущей ветки и уедет в origin."
                  okText="Откатить"
                  okButtonProps={{ danger: true }}
                  cancelText="Нет"
                  onConfirm={() =>
                    rollbackApply.run({ ref: plan.resolved }).then((r) => {
                      if (r) reload()
                    })
                  }
                >
                  <Button danger type="primary" icon={<UndoOutlined />} loading={rollbackApply.loading}>
                    Откатить до этой версии и запушить
                  </Button>
                </Popconfirm>
              </>
            )}
            <ActionError error={rollbackApply.error} />
            {rollbackApply.result && (
              <Alert
                type={rollbackApply.result.ok ? 'success' : 'error'}
                showIcon
                message={rollbackApply.result.ok ? 'Откат выполнен' : 'Откат не прошёл'}
                description={<pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{rollbackApply.result.log}</pre>}
              />
            )}
          </Space>
        </Card>
      )}
    </Space>
  )
}
