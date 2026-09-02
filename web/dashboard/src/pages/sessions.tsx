import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { RefreshCw, Search, Trash2 } from 'lucide-react'

import { api, type SessionInfo } from '@/lib/api'
import { formatTime } from '@/lib/utils'
import { usePolling } from '@/hooks/use-polling'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { ConfirmDialog } from '@/components/confirm-dialog'

export function SessionsPage() {
  const { t } = useTranslation()
  const { data, error, loading, refresh } = usePolling(() => api.sessions(), 5000)
  const [q, setQ] = useState('')
  const [target, setTarget] = useState<SessionInfo | null>(null)

  const needle = q.trim().toLowerCase()
  const rows = (data ?? []).filter(
    (s) =>
      !needle ||
      s.clientId.toLowerCase().includes(needle) ||
      s.username.toLowerCase().includes(needle),
  )

  async function doDelete() {
    if (!target) return
    const id = target.clientId
    try {
      await api.deleteSession(id)
      toast.success(t('sessions.deleted', { id }))
      setTarget(null)
      void refresh()
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e))
    }
  }

  return (
    <div className="flex flex-col gap-4">
      {error && (
        <div className="bg-destructive/10 border-destructive/40 text-destructive rounded-md border px-4 py-3 text-sm">
          {error.message}
        </div>
      )}

      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="relative max-w-xs flex-1">
          <Search className="text-muted-foreground absolute top-1/2 left-2.5 size-4 -translate-y-1/2" />
          <Input
            className="pl-8"
            placeholder={t('common.search')}
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
        <Button variant="outline" size="sm" onClick={() => void refresh()}>
          <RefreshCw className={loading ? 'animate-spin' : undefined} />
          {t('common.refresh')}
        </Button>
      </div>

      <Card className="gap-0 py-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('sessions.clientId')}</TableHead>
              <TableHead>{t('clients.username')}</TableHead>
              <TableHead>{t('clients.version')}</TableHead>
              <TableHead>{t('sessions.connected')}</TableHead>
              <TableHead className="text-right">{t('sessions.sessionExpiry')}</TableHead>
              <TableHead>{t('sessions.createdAt')}</TableHead>
              <TableHead className="text-right">{t('common.actions')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="text-muted-foreground h-24 text-center">
                  {loading ? t('common.loading') : t('common.empty')}
                </TableCell>
              </TableRow>
            ) : (
              rows.map((s) => (
                <TableRow key={s.clientId}>
                  <TableCell className="font-medium">{s.clientId}</TableCell>
                  <TableCell>{s.username || '—'}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{s.version}</Badge>
                  </TableCell>
                  <TableCell>
                    <Badge variant={s.connected ? 'success' : 'secondary'}>
                      {s.connected ? t('common.online') : t('common.offline')}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {s.sessionExpiry > 0 ? `${s.sessionExpiry}s` : '—'}
                  </TableCell>
                  <TableCell className="text-muted-foreground text-xs">
                    {formatTime(s.createdAt)}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-destructive hover:text-destructive"
                      onClick={() => setTarget(s)}
                      title={t('sessions.deleteTitle')}
                    >
                      <Trash2 />
                      <span className="hidden sm:inline">{t('common.delete')}</span>
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>

      <ConfirmDialog
        open={target !== null}
        onOpenChange={(v) => {
          if (!v) setTarget(null)
        }}
        title={t('sessions.deleteTitle')}
        description={target ? t('sessions.deleteDesc', { id: target.clientId }) : undefined}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        destructive
        onConfirm={doDelete}
      />
    </div>
  )
}
