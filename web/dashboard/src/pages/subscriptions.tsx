import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { RefreshCw, Search } from 'lucide-react'

import { api } from '@/lib/api'
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

export function SubscriptionsPage() {
  const { t } = useTranslation()
  const { data, error, loading, refresh } = usePolling(() => api.subscriptions(), 5000)
  const [q, setQ] = useState('')

  const needle = q.trim().toLowerCase()
  const rows = (data ?? []).filter(
    (s) =>
      !needle ||
      s.clientId.toLowerCase().includes(needle) ||
      s.filter.toLowerCase().includes(needle),
  )

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
              <TableHead>{t('subs.clientId')}</TableHead>
              <TableHead>{t('subs.filter')}</TableHead>
              <TableHead className="text-right">{t('subs.qos')}</TableHead>
              <TableHead className="text-right">{t('subs.noLocal')}</TableHead>
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
              rows.map((s) => (
                <TableRow key={`${s.clientId}-${s.filter}`}>
                  <TableCell className="font-medium">{s.clientId}</TableCell>
                  <TableCell className="font-mono text-xs">{s.filter}</TableCell>
                  <TableCell className="text-right">
                    <Badge variant={s.qos > 0 ? 'default' : 'secondary'}>QoS {s.qos}</Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground text-right tabular-nums">
                    {s.noLocal ? t('common.yes') : t('common.no')}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>
    </div>
  )
}
