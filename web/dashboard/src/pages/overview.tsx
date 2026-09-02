import { useEffect, useRef, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  Archive,
  Clock,
  HardDrive,
  Inbox,
  Network,
  Send,
  UserPlus,
  Users,
  type LucideIcon,
} from 'lucide-react'

import { api, type Info, type Stats } from '@/lib/api'
import { formatBytes, formatNumber, formatTime, formatUptime } from '@/lib/utils'
import { usePolling } from '@/hooks/use-polling'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

const MAX_POINTS = 60
const STAT_POLL_MS = 5000
const INFO_POLL_MS = 15000
const HEALTH_POLL_MS = 10000

interface Point {
  label: string
  rx: number
  tx: number
  connected: number
}

interface StatCardProps {
  icon: LucideIcon
  label: string
  value: string
  hint?: string
}

function StatCard({ icon: Icon, label, value, hint }: StatCardProps) {
  return (
    <Card className="py-4">
      <CardContent className="flex items-center gap-3 px-5">
        <div className="bg-primary/10 text-primary flex size-10 shrink-0 items-center justify-center rounded-lg">
          <Icon className="size-5" />
        </div>
        <div className="min-w-0">
          <div className="text-muted-foreground truncate text-xs font-medium">{label}</div>
          <div className="truncate text-xl font-semibold tabular-nums">{value}</div>
          {hint && (
            <div className="text-muted-foreground truncate text-xs tabular-nums">{hint}</div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

function InfoRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 py-1.5">
      <span className="text-muted-foreground shrink-0 text-sm">{label}</span>
      <span className="min-w-0 truncate text-right text-sm font-medium">{value}</span>
    </div>
  )
}

export function OverviewPage() {
  const { t } = useTranslation()
  const statsQ = usePolling(() => api.stats(), STAT_POLL_MS)
  const infoQ = usePolling(() => api.info(), INFO_POLL_MS)

  const [healthy, setHealthy] = useState(true)
  const [points, setPoints] = useState<Point[]>([])
  const lastRef = useRef<{ t: number; rx: number; tx: number } | null>(null)

  useEffect(() => {
    let alive = true
    const check = async () => {
      const ok = await api.healthOk()
      if (alive) setHealthy(ok)
    }
    void check()
    const id = setInterval(() => void check(), HEALTH_POLL_MS)
    return () => {
      alive = false
      clearInterval(id)
    }
  }, [])

  useEffect(() => {
    const s = statsQ.data
    if (!s) return
    const now = Date.now()
    const prev = lastRef.current
    let rx = 0
    let tx = 0
    if (prev) {
      const dt = (now - prev.t) / 1000
      if (dt > 0) {
        rx = Math.max(0, (s.messagesReceived - prev.rx)) / dt
        tx = Math.max(0, (s.messagesSent - prev.tx)) / dt
      }
    }
    lastRef.current = { t: now, rx: s.messagesReceived, tx: s.messagesSent }
    setPoints((cur) => {
      const next = [
        ...cur,
        {
          label: new Date(now).toLocaleTimeString([], { hour12: false }),
          rx,
          tx,
          connected: s.clientsConnected,
        },
      ]
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next
    })
  }, [statsQ.data])

  const st = statsQ.data as Stats | null
  const info = infoQ.data as Info | null
  const n = (v: number | undefined): string => (v == null ? '—' : formatNumber(v))

  return (
    <div className="flex flex-col gap-4">
      {statsQ.error && (
        <div className="bg-destructive/10 border-destructive/40 text-destructive rounded-md border px-4 py-3 text-sm">
          <span className="font-semibold">{t('common.error')}: </span>
          {statsQ.error.message}
        </div>
      )}

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard icon={Users} label={t('overview.connected')} value={n(st?.clientsConnected)} />
        <StatCard
          icon={UserPlus}
          label={t('overview.totalClients')}
          value={n(st?.clientsTotal)}
        />
        <StatCard icon={HardDrive} label={t('overview.sessions')} value={n(st?.sessions)} />
        <StatCard icon={Inbox} label={t('overview.msgsReceived')} value={n(st?.messagesReceived)} />
        <StatCard icon={Send} label={t('overview.msgsSent')} value={n(st?.messagesSent)} />
        <StatCard
          icon={Archive}
          label={t('overview.retained')}
          value={n(st?.retainedMessages)}
          hint={
            st ? `${t('overview.retainedSize')}: ${formatBytes(st.retainedSizeBytes)}` : undefined
          }
        />
        <StatCard
          icon={Clock}
          label={t('overview.uptime')}
          value={st ? formatUptime(st.uptimeSeconds) : '—'}
        />
        <StatCard icon={Network} label={t('overview.nodes')} value={n(st?.nodes.length)} />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>{t('overview.throughput')}</CardTitle>
            <CardDescription>
              {t('overview.received')} / {t('overview.sent')}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {points.length < 2 ? (
              <div className="text-muted-foreground flex h-[260px] items-center justify-center text-sm">
                {t('common.loading')}
              </div>
            ) : (
              <div className="h-[260px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={points} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <XAxis
                      dataKey="label"
                      tick={{ fontSize: 11 }}
                      tickLine={false}
                      axisLine={false}
                      minTickGap={48}
                    />
                    <YAxis
                      width={46}
                      tick={{ fontSize: 11 }}
                      tickLine={false}
                      axisLine={false}
                      allowDecimals={false}
                    />
                    <Tooltip />
                    <Legend />
                    <Area
                      type="monotone"
                      dataKey="rx"
                      name={t('overview.received')}
                      stroke="#3b82f6"
                      fill="#3b82f6"
                      fillOpacity={0.15}
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                    />
                    <Area
                      type="monotone"
                      dataKey="tx"
                      name={t('overview.sent')}
                      stroke="#22c55e"
                      fill="#22c55e"
                      fillOpacity={0.15}
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>{t('overview.clientsChart')}</CardTitle>
            <CardDescription>{t('overview.connected')}</CardDescription>
          </CardHeader>
          <CardContent>
            {points.length < 2 ? (
              <div className="text-muted-foreground flex h-[260px] items-center justify-center text-sm">
                {t('common.loading')}
              </div>
            ) : (
              <div className="h-[260px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={points} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <XAxis
                      dataKey="label"
                      tick={{ fontSize: 11 }}
                      tickLine={false}
                      axisLine={false}
                      minTickGap={48}
                    />
                    <YAxis
                      width={46}
                      tick={{ fontSize: 11 }}
                      tickLine={false}
                      axisLine={false}
                      allowDecimals={false}
                    />
                    <Tooltip />
                    <Line
                      type="monotone"
                      dataKey="connected"
                      name={t('overview.connected')}
                      stroke="#a855f7"
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>{t('overview.brokerInfo')}</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-x-8 md:grid-cols-2">
              <div className="divide-y">
                <InfoRow label={t('overview.nodeId')} value={info?.nodeId ?? '—'} />
                <InfoRow label={t('overview.version')} value={info?.version ?? '—'} />
                <InfoRow
                  label={t('overview.commit')}
                  value={info?.commit ? info.commit.slice(0, 8) : '—'}
                />
                <InfoRow label={t('overview.buildDate')} value={info?.date ?? '—'} />
                <InfoRow
                  label={t('overview.mode')}
                  value={
                    info?.mode ? (
                      <Badge variant={info.mode === 'cluster' ? 'default' : 'secondary'}>
                        {info.mode === 'cluster' ? t('overview.cluster') : t('overview.standalone')}
                      </Badge>
                    ) : (
                      '—'
                    )
                  }
                />
              </div>
              <div className="divide-y">
                <InfoRow
                  label={t('overview.redis')}
                  value={info?.redisAddr ? info.redisAddr : '—'}
                />
                <InfoRow
                  label={t('overview.startedAt')}
                  value={st ? formatTime(st.startedAt) : '—'}
                />
                <InfoRow
                  label={t('overview.nodes')}
                  value={
                    st && st.nodes.length > 0 ? (
                      <span className="flex flex-wrap justify-end gap-1">
                        {st.nodes.map((id) => (
                          <Badge key={id} variant="secondary">
                            {id}
                          </Badge>
                        ))}
                      </span>
                    ) : (
                      '—'
                    )
                  }
                />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>{t('overview.health')}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col items-start gap-3">
            <Badge variant={healthy ? 'success' : 'destructive'}>
              {healthy ? t('overview.healthOk') : t('overview.healthFail')}
            </Badge>
            {!healthy && (
              <p className="text-destructive text-sm">
                {statsQ.error?.message ?? t('overview.healthFail')}
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
