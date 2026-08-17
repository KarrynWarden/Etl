import { Alert, Table, Tag, Typography } from 'antd'

// Сопоставление колонок ведущей и ведомой.
//
// Порядок здесь — не оформление, а смысл: перенос читает строку ведущей и
// пишет её в ведомую ПО ПОЗИЦИЯМ структур, а не по именам. Поэтому пара с
// разными именами (GROUPPCODE / groupcode, DVISIT / dvizit) — это нормально и
// встречается в боевых линиях, а вот сдвиг на одну строку означал бы, что
// данные поедут не в те колонки. Таблица показывает именно пары, чтобы такое
// было видно глазом.
export default function MappingTable({ spec }) {
  const master = spec.master_cols || []
  const slave = spec.slave_cols || []
  const pairs = spec.pairs || []

  const name = (c) => c?.column_name || c?.COLUMN_NAME || ''
  const type = (c) => c?.data_type || c?.DATA_TYPE || ''
  const scale = (c) => {
    const v = c?.data_scale ?? c?.DATA_SCALE
    return v === null || v === undefined ? '' : String(v)
  }
  const isPk = (c) => Boolean(c?.is_primary_key || c?.IS_PRIMARY_KEY)

  const byName = (cols) => {
    const map = new Map()
    cols.forEach((c) => map.set(name(c), c))
    return map
  }
  const masterBy = byName(master)
  const slaveBy = byName(slave)

  const rows = pairs.map(([m, s], i) => ({
    key: i,
    position: i + 1,
    master: masterBy.get(m),
    slave: slaveBy.get(s),
    masterName: m,
    slaveName: s,
  }))

  const mismatched = rows.filter(
    (r) => isPk(r.master) !== isPk(r.slave),
  )

  const columns = [
    { title: '#', dataIndex: 'position', width: 48 },
    {
      title: `Ведущая (${spec.db_master})`,
      render: (_, r) => (
        <span>
          <Typography.Text code>{r.masterName}</Typography.Text>{' '}
          <Typography.Text type="secondary">
            {type(r.master)}
            {scale(r.master) !== '' ? `(${scale(r.master)})` : ''}
          </Typography.Text>{' '}
          {isPk(r.master) && <Tag color="gold">PK</Tag>}
        </span>
      ),
    },
    {
      title: `Ведомая (${spec.db_slave})`,
      render: (_, r) => (
        <span>
          <Typography.Text code>{r.slaveName}</Typography.Text>{' '}
          <Typography.Text type="secondary">
            {type(r.slave)}
            {scale(r.slave) !== '' ? `(${scale(r.slave)})` : ''}
          </Typography.Text>{' '}
          {isPk(r.slave) && <Tag color="gold">PK</Tag>}
        </span>
      ),
    },
    {
      title: 'Имена',
      width: 120,
      render: (_, r) =>
        r.masterName.toLowerCase() === r.slaveName.toLowerCase() ? (
          <Typography.Text type="secondary">совпадают</Typography.Text>
        ) : (
          <Tag>различаются</Tag>
        ),
    },
  ]

  return (
    <>
      {mismatched.length > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="Признак первичного ключа стоит не на обеих сторонах"
          description={
            'Позиции: ' +
            mismatched.map((r) => r.position).join(', ') +
            '. Ключ задаёт ведущая, ведомая колонка той же позиции помечается так же — ' +
            'расхождение означает, что структуры правились врозь.'
          }
        />
      )}
      <Table
        size="small"
        bordered
        pagination={false}
        scroll={{ y: 420 }}
        dataSource={rows}
        columns={columns}
      />
      <Typography.Paragraph type="secondary" style={{ marginTop: 8 }}>
        Сопоставлено колонок: {rows.length}. Ведущая знает {master.length}, ведомая{' '}
        {slave.length}.
      </Typography.Paragraph>
    </>
  )
}
