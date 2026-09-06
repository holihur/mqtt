import { useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Pause, Play, RefreshCw } from 'lucide-react'

import { api, type LogEntry } from '@/lib/api'
import { usePolling } from '@/hooks/use-polling'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'

const LEVELS = ['debug', 'info', 'warn', 'error'] as const

function levelBadge(level: string) {
  switch (level) {
    case 'debug':
      return <Badge variant="secondary">debug</Badge>
    case 'info':
      return <Badge variant="default">info</Badge>
    case 'warn':
      return <Badge className="bg-amber-500/15 text-amber-600 dark:text-amber-400">warn</Badge>
    case 'error':
      return <Badge variant="destructive">error</Badge>
    default:
      return <Badge variant="outline">{level}</Badge>
  }
}

export function LogsPage() {
  const { t } = useTranslation()
  const [level, setLevel] = useState<string>('')
  const [limit, setLimit] = useState(300)
  const [autoScroll, setAutoScroll] = useState(true)
  const [paused, setPaused] = useState(false)
  const boxRef = useRef<HTMLDivElement>(null)

  const { data, error, loading, refresh } = usePolling(
    () => api.logs(limit, level || undefined),
    paused ? 0 : 3000,
  )

  useEffect(() => {
    if (autoScroll && boxRef.current) {
      boxRef.current.scrollTop = boxRef.current.scrollHeight
    }
  }, [data, autoScroll])

  const rows = data ?? []

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-1 rounded-md border p-1">
          <button
            type="button"
            onClick={() => setLevel('')}
            className={cn(
              'rounded px-2.5 py-1 text-xs font-medium transition-colors',
              level === '' ? 'bg-primary text-primary-foreground' : 'hover:bg-accent',
            )}
          >
            all
          </button>
          {LEVELS.map((v) => (
            <button
              key={v}
              type="button"
              onClick={() => setLevel(v)}
              className={cn(
                'rounded px-2.5 py-1 text-xs font-medium transition-colors',
                level === v ? 'bg-primary text-primary-foreground' : 'hover:bg-accent',
              )}
            >
              {v}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <select
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
            className="border-input bg-background h-8 rounded-md border px-2 text-sm"
          >
            {[100, 300, 500, 1000, 2000].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
          <Button variant="outline" size="sm" onClick={() => setPaused((p) => !p)}>
            {paused ? <Play /> : <Pause />}
            {paused ? t('logs.resume') : t('logs.pause')}
          </Button>
          <Button variant="outline" size="sm" onClick={() => void refresh()}>
            <RefreshCw className={loading ? 'animate-spin' : undefined} />
            {t('common.refresh')}
          </Button>
        </div>
      </div>

      {error && (
        <div className="bg-destructive/10 border-destructive/40 text-destructive rounded-md border px-4 py-3 text-sm">
          {error.message}
        </div>
      )}

      <label className="flex items-center gap-2 text-sm">
        <input
          type="checkbox"
          checked={autoScroll}
          onChange={(e) => setAutoScroll(e.target.checked)}
        />
        {t('logs.autoScroll')}
      </label>

      <Card className="gap-0 py-0">
        <div
          ref={boxRef}
          className="bg-muted/30 max-h-[65vh] overflow-auto rounded-md p-2 font-mono text-xs"
        >
          {rows.length === 0 ? (
            <div className="text-muted-foreground flex h-24 items-center justify-center">
              {loading ? t('common.loading') : t('common.empty')}
            </div>
          ) : (
            rows.map((e: LogEntry, i) => (
              <div key={i} className="hover:bg-accent/50 flex gap-2 rounded px-2 py-0.5">
                <span className="text-muted-foreground shrink-0 tabular-nums">
                  {new Date(e.time).toLocaleTimeString()}
                </span>
                <span className="shrink-0">{levelBadge(e.level)}</span>
                <span className="break-all whitespace-pre-wrap">{e.message}</span>
                {e.attrs && Object.keys(e.attrs).length > 0 && (
                  <span className="text-muted-foreground shrink-0">
                    {Object.entries(e.attrs)
                      .map(([k, v]) => `${k}=${v}`)
                      .join(' ')}
                  </span>
                )}
              </div>
            ))
          )}
        </div>
      </Card>
    </div>
  )
}
