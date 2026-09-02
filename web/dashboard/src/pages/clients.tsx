import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { RefreshCw, Search, UserX } from 'lucide-react'

import { api, type ClientInfo } from '@/lib/api'
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

export function ClientsPage() {
  const { t } = useTranslation()
  const { data, error, loading, refresh } = usePolling(() => api.clients(), 3000)
  const [q, setQ] = useState('')
  const [kick, setKick] = useState<ClientInfo | null>(null)

  const needle = q.trim().toLowerCase()
  const rows = (data ?? []).filter(
    (c) =>
      !needle ||
      c.clientId.toLowerCase().includes(needle) ||
      c.username.toLowerCase().includes(needle),
  )

  async function doKick() {
    if (!kick) return
    const id = kick.clientId
    try {
      await api.kickClient(id)
      toast.success(t('clients.kicked', { id }))
      setKick(null)
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
              <TableHead>{t('clients.clientId')}</TableHead>
              <TableHead>{t('clients.username')}</TableHead>
              <TableHead>{t('clients.version')}</TableHead>
              <TableHead>{t('clients.remoteAddr')}</TableHead>
              <TableHead className="text-right">{t('clients.keepAlive')}</TableHead>
              <TableHead className="text-right">{t('clients.subscriptions')}</TableHead>
              <TableHead className="text-right">{t('clients.inflight')}</TableHead>
              <TableHead>{t('clients.node')}</TableHead>
              <TableHead>{t('clients.connectedAt')}</TableHead>
              <TableHead className="text-right">{t('common.actions')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={10} className="text-muted-foreground h-24 text-center">
                  {loading ? t('common.loading') : t('common.empty')}
                </TableCell>
              </TableRow>
            ) : (
              rows.map((c) => (
                <TableRow key={c.clientId}>
                  <TableCell className="font-medium">{c.clientId}</TableCell>
                  <TableCell>{c.username || '—'}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{c.version}</Badge>
                  </TableCell>
                  <TableCell className="font-mono text-xs">{c.remoteAddr}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {c.keepAlive > 0 ? `${c.keepAlive}s` : '—'}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{c.subscriptions}</TableCell>
                  <TableCell className="text-right tabular-nums">{c.inflight}</TableCell>
                  <TableCell className="text-xs">{c.nodeId}</TableCell>
                  <TableCell className="text-muted-foreground text-xs">
                    {formatTime(c.connectedAt)}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-destructive hover:text-destructive"
                      onClick={() => setKick(c)}
                      title={t('clients.kick')}
                    >
                      <UserX />
                      <span className="hidden sm:inline">{t('clients.kick')}</span>
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>

      <ConfirmDialog
        open={kick !== null}
        onOpenChange={(v) => {
          if (!v) setKick(null)
        }}
        title={t('clients.kickTitle')}
        description={kick ? t('clients.kickDesc', { id: kick.clientId }) : undefined}
        confirmLabel={t('clients.kick')}
        cancelLabel={t('common.cancel')}
        destructive
        onConfirm={doKick}
      />
    </div>
  )
}
