import { Alert, Button, Modal, Radio, Space, Table, Tag, Typography } from 'antd'

// Разбор того, что вернула БД, ПЕРЕД тем как это применить.
//
// Раньше «Снять структуры из БД» просто заменяло обе стороны и подбирало пары
// заново по именам. Для линии, у которой пары расставлены руками, это потеря:
// в prbdir ключи зовутся idrw и prvdirid, и автоподбор по именам такую пару не
// восстановит; ещё хуже случай acc → bancacc, когда в ведомой есть и bancacc, и
// свой acc — автоподбор уверенно свяжет acc с acc и молча испортит перенос.
// А нажимают эту кнопку обычно ради ОДНОЙ новой колонки.
//
// Поэтому снятие ничего не меняет само: оно показывает список отличий, и каждое
// принимается или отклоняется отдельно. Не тронутое человеком считается
// ОТКЛОНЁННЫМ — по умолчанию не меняется ничего, и «принял не глядя» не
// получается случайно.
const KIND_TEXT = {
  master_added: 'новая колонка ведущей',
  master_removed: 'колонки ведущей больше нет',
  master_type: 'другой тип у ведущей',
  master_pk: 'другой признак ключа у ведущей',
  slave_added: 'новая колонка ведомой',
  slave_removed: 'колонки ведомой больше нет',
  slave_type: 'другой тип у ведомой',
}
const KIND_COLOR = {
  master_added: 'green',
  slave_added: 'green',
  master_removed: 'red',
  slave_removed: 'red',
  master_type: 'blue',
  slave_type: 'blue',
  master_pk: 'orange',
}

export default function SnapReview({ open, changes, decisions, onDecide, onApply, onCancel }) {
  const accepted = Object.values(decisions).filter((d) => d === 'yes').length
  const rows = changes || []

  return (
    <Modal
      open={open}
      width={1000}
      title="Что предлагает база"
      onCancel={onCancel}
      footer={
        <Space>
          <Button onClick={onCancel}>Ничего не менять</Button>
          <Button
            onClick={() => onDecide('__all__', 'yes')}
            disabled={!rows.length}
          >
            Принять всё
          </Button>
          <Button type="primary" disabled={!accepted} onClick={onApply}>
            Применить принятое ({accepted})
          </Button>
        </Space>
      }
    >
      {!rows.length ? (
        <Alert
          type="success"
          showIcon
          message="Структуры совпали"
          description="То, что вернула база, ничем не отличается от того, что записано у линии. Менять нечего."
        />
      ) : (
        <>
          <Alert
            type="info"
            showIcon
            style={{ marginBottom: 12 }}
            message="Сопоставление колонок не трогается"
            description="Принятые изменения правят только то, что перечислено ниже. Пары, расставленные руками — idrw ↔ prvdirid, acc ↔ bancacc — остаются как есть; у новой колонки пара предлагается, но её можно поменять после применения."
          />
          <Table
            size="small"
            bordered
            rowKey="id"
            pagination={false}
            scroll={{ y: 420 }}
            dataSource={rows}
            columns={[
              {
                title: 'Что',
                width: 230,
                render: (_v, r) => (
                  <Space direction="vertical" size={0}>
                    <Tag color={KIND_COLOR[r.kind]}>{KIND_TEXT[r.kind]}</Tag>
                    <Typography.Text code>{r.name}</Typography.Text>
                  </Space>
                ),
              },
              {
                title: 'Сейчас у линии',
                render: (_v, r) => (
                  <Typography.Text type={r.before === undefined ? 'secondary' : undefined}>
                    {r.before === undefined ? '— нет —' : String(r.before)}
                  </Typography.Text>
                ),
              },
              {
                title: 'В базе',
                render: (_v, r) => (
                  <Typography.Text type={r.after === undefined ? 'secondary' : undefined}>
                    {r.after === undefined ? '— нет —' : String(r.after)}
                  </Typography.Text>
                ),
              },
              {
                title: 'Что делаем',
                width: 260,
                render: (_v, r) => (
                  <Radio.Group
                    size="small"
                    value={decisions[r.id] || 'no'}
                    onChange={(e) => onDecide(r.id, e.target.value)}
                  >
                    <Radio.Button value="yes">принять</Radio.Button>
                    <Radio.Button value="no">оставить как есть</Radio.Button>
                  </Radio.Group>
                ),
              },
            ]}
          />
          <Typography.Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
            Не отмеченное «принять» не применяется. Удаление колонки ведущей
            уберёт и её пару из сопоставления.
          </Typography.Paragraph>
        </>
      )}
    </Modal>
  )
}
