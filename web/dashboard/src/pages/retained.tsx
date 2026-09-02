import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { Eye, RefreshCw, Search, Trash2 } from 'lucide-react'

import { api, type RetainedMessage } from '@/lib/api'
import { decodePayload, formatBytes, type DecodedPayload } from '@/lib/utils'
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { ConfirmDialog } from '@/components/confirm-dialog'

interface ViewState {
  topic: string
  size: number
  decoded?: DecodedPayload
}

export function RetainedPage() {
  const { t } = useTranslation()
  const { data, error, loading, refresh } = usePolling(() => api.retained(false), 8000)
  const [q, setQ] = useState('')
  const [delTarget, setDelTarget] = useState<RetainedMessage | null>(null)
  const [clearOpen, setClearOpen] = useState(false)
  const [view, setView] = useState<ViewState | null>(null)
  const [viewBusy, setViewBusy] = useState(false)

  const needle = q.trim().toLowerCase()
  const rows = (data ?? []).filter((r) => !needle || r.topic.toLowerCase().includes(needle))
  const count = data?.length ?? 0

  async function openView(row: RetainedMessage) {
    setView({ topic: row.topic, size: row.size })
    setViewBusy(true)
    try {
      const all = await api.retained(true)
      const found = all.find((r) => r.topic === row.topic)
      setView((v) =>
        v && found?.payloadB64
          ? { ...v, decoded: decodePayload(found.payloadB64) }
          : v
            ? { ...v, decoded: { kind: 'text', value: '' } }
            : v,
      )
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e))
    } finally {
      setViewBusy(false)
    }
  }

  async function doDelete() {
    if (!delTarget) return
    const topic = delTarget.topic
    try {
      await api.deleteRetained(topic)
      toast.success(t('retained.deleted'))
      setDelTarget(null)
      void refresh()
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e))
    }
  }

  async function doClear() {
    try {
      const res = await api.clearRetained()
      toast.success(t('retained.cleared', { count: res.deleted ?? 0 }))
      setClearOpen(false)
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
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => void refresh()}>
            <RefreshCw className={loading ? 'animate-spin' : undefined} />
            {t('common.refresh')}
          </Button>
          <Button
            variant="destructive"
            size="sm"
            disabled={count === 0}
            onClick={() => setClearOpen(true)}
          >
            <Trash2 />
            {t('retained.clearAll')}
          </Button>
        </div>
      </div>

      <Card className="gap-0 py-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('retained.topic')}</TableHead>
              <TableHead className="text-right">{t('retained.qos')}</TableHead>
              <TableHead className="text-right">{t('retained.size')}</TableHead>
              <TableHead className="text-right">{t('common.actions')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} className="text-muted-foreground h-24 text-center">
                  {loading ? t('common.loading') : t('common.empty')}
                </TableCell>
              </TableRow>
            ) : (
              rows.map((r) => (
                <TableRow key={r.topic}>
                  <TableCell className="font-mono text-xs font-medium">{r.topic}</TableCell>
                  <TableCell className="text-right">
                    <Badge variant={r.qos > 0 ? 'default' : 'secondary'}>QoS {r.qos}</Badge>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{formatBytes(r.size)}</TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-1">
                      <Button
                        variant="outline"
                        size="icon"
                        className="size-8"
                        onClick={() => void openView(r)}
                        title={t('retained.view')}
                      >
                        <Eye />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="text-destructive hover:text-destructive size-8"
                        onClick={() => setDelTarget(r)}
                        title={t('retained.deleteTitle')}
                      >
                        <Trash2 />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>

      <Dialog
        open={view !== null}
        onOpenChange={(v) => {
          if (!v) setView(null)
        }}
      >
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>{view?.topic}</DialogTitle>
            <DialogDescription>
              {t('retained.payload')} · {view ? formatBytes(view.size) : ''}
            </DialogDescription>
          </DialogHeader>
          {view?.decoded ? (
            <pre className="bg-muted/50 border-border text-foreground max-h-80 overflow-auto rounded-md border p-3 font-mono text-xs break-all whitespace-pre-wrap">
              {view.decoded.value}
            </pre>
          ) : (
            <div className="text-muted-foreground flex h-24 items-center justify-center text-sm">
              {viewBusy ? t('common.loading') : ''}
            </div>
          )}
        </DialogContent>
      </Dialog>

      <ConfirmDialog
        open={delTarget !== null}
        onOpenChange={(v) => {
          if (!v) setDelTarget(null)
        }}
        title={t('retained.deleteTitle')}
        description={delTarget ? t('retained.deleteDesc', { topic: delTarget.topic }) : undefined}
        confirmLabel={t('common.delete')}
        cancelLabel={t('common.cancel')}
        destructive
        onConfirm={doDelete}
      />

      <ConfirmDialog
        open={clearOpen}
        onOpenChange={setClearOpen}
        title={t('retained.clearTitle')}
        description={t('retained.clearDesc', { count })}
        confirmLabel={t('retained.clearAll')}
        cancelLabel={t('common.cancel')}
        destructive
        onConfirm={doClear}
      />
    </div>
  )
}
