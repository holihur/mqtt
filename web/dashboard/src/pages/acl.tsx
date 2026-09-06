import { useCallback, useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { RefreshCw, Save } from 'lucide-react'

import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Textarea } from '@/components/ui/textarea'

export function AclPage() {
  const { t } = useTranslation()
  const [path, setPath] = useState('')
  const [content, setContent] = useState('')
  const [rules, setRules] = useState(0)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const f = await api.aclFile()
      setPath(f.path)
      setContent(f.content)
      setRules(f.rules)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  async function save() {
    setSaving(true)
    try {
      const res = await api.saveAclFile(content)
      setRules(res.rules ?? 0)
      toast.success(t('acl.saved', { count: res.rules ?? 0 }))
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="flex max-w-4xl flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>{t('acl.title')}</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground font-mono">{path || t('acl.noFile')}</span>
            <span className="text-muted-foreground">
              {t('acl.rules')}: {rules}
            </span>
          </div>
          <Textarea
            className="min-h-96 font-mono text-xs"
            value={content}
            onChange={(e) => setContent(e.target.value)}
            placeholder="# user alice\n# topic sensors/#\n# ..."
            spellCheck={false}
          />
          <div className="flex gap-2">
            <Button onClick={() => void save()} disabled={saving || loading}>
              <Save className={saving ? 'animate-pulse' : undefined} />
              {t('acl.save')}
            </Button>
            <Button variant="outline" onClick={() => void load()} disabled={loading}>
              <RefreshCw className={loading ? 'animate-spin' : undefined} />
              {t('common.refresh')}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
