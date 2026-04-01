import { useEffect, useState, useCallback, useRef } from 'react'
import {
  Card,
  Table,
  Button,
  Modal,
  Drawer,
  Form,
  Input,
  InputNumber,
  Select,
  Switch,
  Space,
  Tag,
  Popconfirm,
  message,
  Typography,
  Tooltip,
} from 'antd'
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  ClockCircleOutlined,
  FileTextOutlined,
} from '@ant-design/icons'
import { apiFetch } from '@/lib/utils'

const { Text } = Typography

interface ScheduledJob {
  id: number
  name: string
  enabled: boolean
  job_type: string
  platform: string
  count: number
  concurrency: number
  register_delay_seconds: number
  cron_expr: string
  interval_minutes: number
  last_run_at: string | null
  next_run_at: string | null
  last_task_id: string
  last_status: string
  created_at: string | null
  updated_at: string | null
}

const JOB_TYPE_OPTIONS = [
  { value: 'register', label: '定时注册' },
  { value: 'cpa_clean', label: 'CPA 检测清理' },
]

const PLATFORM_OPTIONS = [
  { value: 'chatgpt', label: 'ChatGPT' },
  { value: 'trae', label: 'Trae.ai' },
  { value: 'cursor', label: 'Cursor' },
  { value: 'kiro', label: 'Kiro' },
  { value: 'grok', label: 'Grok' },
  { value: 'tavily', label: 'Tavily' },
  { value: 'openblocklabs', label: 'OpenBlockLabs' },
]

const SCHEDULE_PRESETS = [
  { label: '每 30 分钟', value: 'interval:30' },
  { label: '每 1 小时', value: 'interval:60' },
  { label: '每 2 小时', value: 'interval:120' },
  { label: '每 4 小时', value: 'interval:240' },
  { label: '每 6 小时', value: 'interval:360' },
  { label: '每 12 小时', value: 'interval:720' },
  { label: '每天一次', value: 'interval:1440' },
  { label: '自定义 Cron', value: 'cron' },
  { label: '自定义间隔', value: 'custom_interval' },
]

function formatDateTime(iso: string | null) {
  if (!iso) return '-'
  try {
    return new Date(iso).toLocaleString('zh-CN', { hour12: false })
  } catch {
    return iso
  }
}

function describeSchedule(job: ScheduledJob) {
  if (job.interval_minutes > 0) {
    if (job.interval_minutes < 60) return `每 ${job.interval_minutes} 分钟`
    if (job.interval_minutes % 60 === 0) return `每 ${job.interval_minutes / 60} 小时`
    return `每 ${job.interval_minutes} 分钟`
  }
  if (job.cron_expr) return `Cron: ${job.cron_expr}`
  return '未设置'
}

export default function ScheduledJobs() {
  const [jobs, setJobs] = useState<ScheduledJob[]>([])
  const [loading, setLoading] = useState(false)
  const [modalOpen, setModalOpen] = useState(false)
  const [editing, setEditing] = useState<ScheduledJob | null>(null)
  const [form] = Form.useForm()
  const [logDrawer, setLogDrawer] = useState<{ open: boolean; taskId: string; logs: string[]; status: string; error: string; success: number | null; errors: string[] }>({
    open: false, taskId: '', logs: [], status: '', error: '', success: null, errors: [],
  })

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const data = await apiFetch('/scheduled-jobs')
      setJobs(data)
    } catch (e: any) {
      message.error(`加载失败: ${e.message}`)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
    const timer = setInterval(load, 10000)
    return () => clearInterval(timer)
  }, [load])

  const openCreate = () => {
    setEditing(null)
    form.resetFields()
    form.setFieldsValue({
      name: '',
      enabled: true,
      job_type: 'register',
      platform: 'chatgpt',
      count: 1,
      concurrency: 1,
      register_delay_seconds: 0,
      schedule_type: 'interval:60',
      interval_minutes: 60,
      cron_expr: '',
    })
    setModalOpen(true)
  }

  const openEdit = (job: ScheduledJob) => {
    setEditing(job)
    let schedType = 'custom_interval'
    if (job.cron_expr) {
      schedType = 'cron'
    } else if (job.interval_minutes > 0) {
      const presetMatch = SCHEDULE_PRESETS.find(
        (p) => p.value === `interval:${job.interval_minutes}`
      )
      schedType = presetMatch ? presetMatch.value : 'custom_interval'
    }
    form.setFieldsValue({
      name: job.name,
      enabled: job.enabled,
      job_type: job.job_type || 'register',
      platform: job.platform,
      count: job.count,
      concurrency: job.concurrency,
      register_delay_seconds: job.register_delay_seconds || 0,
      schedule_type: schedType,
      interval_minutes: job.interval_minutes,
      cron_expr: job.cron_expr,
    })
    setModalOpen(true)
  }

  const handleSave = async () => {
    try {
      const values = await form.validateFields()
      let intervalMin = 0
      let cronExpr = ''

      if (values.schedule_type === 'cron') {
        cronExpr = values.cron_expr || ''
      } else if (values.schedule_type === 'custom_interval') {
        intervalMin = values.interval_minutes || 60
      } else if (values.schedule_type?.startsWith('interval:')) {
        intervalMin = parseInt(values.schedule_type.split(':')[1], 10)
      }

      const payload = {
        name: values.name,
        enabled: values.enabled,
        job_type: values.job_type,
        platform: values.platform,
        count: values.job_type === 'cpa_clean' ? 0 : values.count,
        concurrency: values.concurrency,
        register_delay_seconds: values.job_type === 'cpa_clean' ? 0 : (values.register_delay_seconds || 0),
        cron_expr: cronExpr,
        interval_minutes: intervalMin,
      }

      if (editing) {
        await apiFetch(`/scheduled-jobs/${editing.id}`, {
          method: 'PUT',
          body: JSON.stringify(payload),
        })
        message.success('已更新')
      } else {
        await apiFetch('/scheduled-jobs', {
          method: 'POST',
          body: JSON.stringify(payload),
        })
        message.success('已创建')
      }
      setModalOpen(false)
      load()
    } catch (e: any) {
      message.error(`保存失败: ${e.message}`)
    }
  }

  const handleDelete = async (id: number) => {
    try {
      await apiFetch(`/scheduled-jobs/${id}`, { method: 'DELETE' })
      message.success('已删除')
      load()
    } catch (e: any) {
      message.error(`删除失败: ${e.message}`)
    }
  }

  const handleTrigger = async (id: number) => {
    try {
      await apiFetch(`/scheduled-jobs/${id}/trigger`, { method: 'POST' })
      message.success('已触发')
      load()
      openJobLog(id)
    } catch (e: any) {
      message.error(`触发失败: ${e.message}`)
    }
  }

  const handleToggle = async (id: number, enabled: boolean) => {
    try {
      await apiFetch(`/scheduled-jobs/${id}`, {
        method: 'PUT',
        body: JSON.stringify({ enabled }),
      })
      load()
    } catch (e: any) {
      message.error(`切换失败: ${e.message}`)
    }
  }

  const logPollRef = useRef<number | null>(null)
  const logJobIdRef = useRef<number | null>(null)

  const fetchLog = useCallback(async (jobId: number) => {
    try {
      const data = await apiFetch(`/scheduled-jobs/${jobId}/logs`)
      setLogDrawer((prev) => ({
        ...prev,
        taskId: data.task_id || '',
        logs: data.logs || [],
        status: data.status || '',
        error: data.error || '',
        success: data.success ?? null,
        errors: data.errors || [],
      }))
      return data.status
    } catch {
      return 'error'
    }
  }, [])

  const startLogPoll = useCallback((jobId: number) => {
    if (logPollRef.current) clearInterval(logPollRef.current)
    logJobIdRef.current = jobId
    logPollRef.current = window.setInterval(async () => {
      const status = await fetchLog(jobId)
      if (status === 'done' || status === 'failed' || status === 'error' || status === 'success') {
        if (logPollRef.current) {
          clearInterval(logPollRef.current)
          logPollRef.current = null
        }
      }
    }, 1500)
  }, [fetchLog])

  const stopLogPoll = useCallback(() => {
    if (logPollRef.current) {
      clearInterval(logPollRef.current)
      logPollRef.current = null
    }
  }, [])

  useEffect(() => () => stopLogPoll(), [stopLogPoll])

  const openJobLog = async (jobId: number) => {
    setLogDrawer({ open: true, taskId: '', logs: [], status: 'loading', error: '', success: null, errors: [] })
    const status = await fetchLog(jobId)
    if (status === 'error') {
      setLogDrawer((prev) => ({ ...prev, status: '', logs: [] }))
      message.warning('暂无日志')
      return
    }
    if (status === 'pending' || status === 'running') {
      startLogPoll(jobId)
    }
  }

  const closeLog = () => {
    stopLogPoll()
    setLogDrawer((prev) => ({ ...prev, open: false }))
  }

  const scheduleType = Form.useWatch('schedule_type', form)
  const jobType = Form.useWatch('job_type', form)

  const columns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string, record: ScheduledJob) => (
        <Space>
          <Text strong>{name || `任务 #${record.id}`}</Text>
          {!record.enabled && <Tag>已禁用</Tag>}
        </Space>
      ),
    },
    {
      title: '类型',
      key: 'job_type',
      width: 120,
      render: (_: any, record: ScheduledJob) => {
        if (record.job_type === 'cpa_clean') {
          return <Tag color="orange">CPA 清理</Tag>
        }
        const opt = PLATFORM_OPTIONS.find((o) => o.value === record.platform)
        return <Tag color="blue">{opt?.label || record.platform} 注册</Tag>
      },
    },
    {
      title: '数量 / 并行',
      key: 'count',
      width: 100,
      align: 'center' as const,
      render: (_: any, record: ScheduledJob) => (
        record.job_type === 'cpa_clean'
          ? <span>并行 {record.concurrency}</span>
          : <span>{record.count} / {record.concurrency}</span>
      ),
    },
    {
      title: '调度规则',
      key: 'schedule',
      width: 140,
      render: (_: any, record: ScheduledJob) => (
        <Tooltip title={record.cron_expr || `${record.interval_minutes} 分钟`}>
          <ClockCircleOutlined style={{ marginRight: 4 }} />
          {describeSchedule(record)}
        </Tooltip>
      ),
    },
    {
      title: '上次执行',
      key: 'last_run',
      width: 180,
      render: (_: any, record: ScheduledJob) => (
        <Space direction="vertical" size={0}>
          <Text style={{ fontSize: 12 }}>{formatDateTime(record.last_run_at)}</Text>
          {record.last_status && (
            <Tag
              color={
                record.last_status === 'success'
                  ? 'green'
                  : record.last_status === 'running'
                  ? 'blue'
                  : 'red'
              }
              style={{ fontSize: 11, cursor: record.last_task_id ? 'pointer' : 'default' }}
              onClick={() => record.last_task_id && openJobLog(record.id)}
            >
              {record.last_status}
              {record.last_task_id && <FileTextOutlined style={{ marginLeft: 4 }} />}
            </Tag>
          )}
        </Space>
      ),
    },
    {
      title: '下次执行',
      key: 'next_run',
      width: 160,
      render: (_: any, record: ScheduledJob) => (
        <Text style={{ fontSize: 12, color: record.enabled ? undefined : '#999' }}>
          {record.enabled ? formatDateTime(record.next_run_at) : '已暂停'}
        </Text>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 260,
      render: (_: any, record: ScheduledJob) => (
        <Space size={4}>
          <Switch
            size="small"
            checked={record.enabled}
            onChange={(v) => handleToggle(record.id, v)}
          />
          <Button
            size="small"
            type="link"
            icon={<PlayCircleOutlined />}
            onClick={() => handleTrigger(record.id)}
          >
            触发
          </Button>
          <Button
            size="small"
            type="link"
            icon={<FileTextOutlined />}
            disabled={!record.last_task_id}
            onClick={() => openJobLog(record.id)}
          >
            日志
          </Button>
          <Button
            size="small"
            type="link"
            icon={<EditOutlined />}
            onClick={() => openEdit(record)}
          />
          <Popconfirm title="确认删除？" onConfirm={() => handleDelete(record.id)}>
            <Button size="small" type="link" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 24, fontWeight: 'bold', margin: 0 }}>定时任务</h1>
        <p style={{ color: '#7a8ba3', marginTop: 4 }}>
          设置定时自动注册任务，邮箱 / 执行器 / 验证码 / 代理等参数使用全局配置
        </p>
      </div>

      <Card
        extra={
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            新建定时任务
          </Button>
        }
      >
        <Table
          dataSource={jobs}
          columns={columns}
          rowKey="id"
          loading={loading}
          pagination={false}
          locale={{ emptyText: '暂无定时任务，点击「新建定时任务」创建' }}
        />
      </Card>

      <Modal
        title={editing ? '编辑定时任务' : '新建定时任务'}
        open={modalOpen}
        onOk={handleSave}
        onCancel={() => setModalOpen(false)}
        width={520}
        okText="保存"
        cancelText="取消"
        destroyOnClose
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="name" label="任务名称">
            <Input placeholder={jobType === 'cpa_clean' ? 'CPA 检测清理' : 'ChatGPT 每小时注册'} />
          </Form.Item>
          <Space style={{ width: '100%' }} align="start">
            <Form.Item name="enabled" label="启用" valuePropName="checked">
              <Switch />
            </Form.Item>
            <Form.Item name="job_type" label="任务类型" rules={[{ required: true }]} style={{ width: 200 }}>
              <Select options={JOB_TYPE_OPTIONS} />
            </Form.Item>
          </Space>

          {jobType !== 'cpa_clean' && (
            <Form.Item name="platform" label="平台" rules={[{ required: true }]}>
              <Select options={PLATFORM_OPTIONS} />
            </Form.Item>
          )}

          <Space style={{ width: '100%' }} align="start">
            {jobType !== 'cpa_clean' && (
              <Form.Item name="count" label="每次注册数量" style={{ width: 140 }}>
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            )}
            <Form.Item
              name="concurrency"
              label={jobType === 'cpa_clean' ? '检查并发数' : '并行数'}
              style={{ width: 140 }}
            >
              <InputNumber min={1} style={{ width: '100%' }} />
            </Form.Item>
            {jobType !== 'cpa_clean' && (
              <Form.Item name="register_delay_seconds" label="每个注册延迟(秒)" style={{ width: 160 }}>
                <InputNumber min={0} step={0.5} style={{ width: '100%' }} />
              </Form.Item>
            )}
          </Space>

          {jobType === 'cpa_clean' && (
            <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 16, fontSize: 12 }}>
              CPA 清理流程：拉取 codex auth files → 并发检查有效性 → 禁用 401 文件 → 删除已禁用文件。需在全局配置中设置 CPA API URL 和 Key。
            </Typography.Text>
          )}

          <Card size="small" title="调度设置" style={{ marginBottom: 0 }}>
            <Form.Item name="schedule_type" label="调度方式">
              <Select options={SCHEDULE_PRESETS} />
            </Form.Item>
            {scheduleType === 'cron' && (
              <Form.Item
                name="cron_expr"
                label="Cron 表达式"
                extra="格式：分 时 日 月 周（如 0 */2 * * * 表示每2小时整点）"
                rules={[{ required: true, message: '请输入 Cron 表达式' }]}
              >
                <Input placeholder="0 */2 * * *" style={{ fontFamily: 'monospace' }} />
              </Form.Item>
            )}
            {scheduleType === 'custom_interval' && (
              <Form.Item
                name="interval_minutes"
                label="间隔（分钟）"
                rules={[{ required: true, message: '请输入间隔分钟数' }]}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            )}
          </Card>
        </Form>
      </Modal>

      <Drawer
        title={
          <Space>
            <span>任务日志</span>
            <Text copyable style={{ fontSize: 12, fontFamily: 'monospace' }}>
              {logDrawer.taskId}
            </Text>
            {logDrawer.status && (
              <Tag color={logDrawer.status === 'done' ? 'green' : logDrawer.status === 'running' ? 'blue' : 'red'}>
                {logDrawer.status}
              </Tag>
            )}
          </Space>
        }
        open={logDrawer.open}
        onClose={closeLog}
        width={680}
      >
        {logDrawer.success !== null && (
          <div style={{ marginBottom: 12, display: 'flex', gap: 12 }}>
            <Tag color="green">成功: {logDrawer.success}</Tag>
            {logDrawer.errors.length > 0 && <Tag color="red">失败: {logDrawer.errors.length}</Tag>}
          </div>
        )}
        {logDrawer.error && (
          <div style={{ marginBottom: 12, padding: '8px 12px', background: '#fff2f0', borderRadius: 6, color: '#cf1322', fontSize: 13 }}>
            {logDrawer.error}
          </div>
        )}
        <div
          ref={(el) => { if (el) el.scrollTop = el.scrollHeight }}
          style={{
            background: '#141414',
            color: '#d4d4d4',
            borderRadius: 8,
            padding: '12px 16px',
            fontSize: 12,
            fontFamily: 'monospace',
            lineHeight: 1.8,
            maxHeight: 'calc(100vh - 220px)',
            overflow: 'auto',
          }}
        >
          {logDrawer.logs.length > 0 ? (
            logDrawer.logs.map((line, i) => (
              <div
                key={i}
                style={{
                  color: line.includes('✓') || line.includes('成功')
                    ? '#52c41a'
                    : line.includes('✗') || line.includes('失败') || line.includes('错误')
                    ? '#ff4d4f'
                    : line.includes('步骤')
                    ? '#91caff'
                    : '#d4d4d4',
                }}
              >
                {line}
              </div>
            ))
          ) : (
            <Text type="secondary">暂无日志</Text>
          )}
        </div>
      </Drawer>
    </div>
  )
}
