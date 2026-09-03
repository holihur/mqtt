import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { RefreshCw, Save } from 'lucide-react'

import { api, getApiBase, getToken, getWsUrl, setApiBase, setToken, setWsUrl } from '@/lib/api'
import { changeLanguage } from '@/lib/i18n'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Separator } from '@/components/ui/separator'

interface SettingsProps {
  theme: 'light' | 'dark'
  onThemeChange: (theme: 'light' | 'dark') => void
}

export function SettingsPage({ theme, onThemeChange }: SettingsProps) {
  const { t, i18n } = useTranslation()
  const [apiBase, setApiBaseState] = useState(getApiBase())
  const [token, setTokenState] = useState(getToken())
  const [wsUrl, setWsUrlState] = useState(getWsUrl())
  const [aclBusy, setAclBusy] = useState(false)

  const lang = i18n.language.toLowerCase().startsWith('zh') ? 'zh' : 'en'

  function saveConnection() {
    setApiBase(apiBase)
    setToken(token.trim())
    setWsUrl(wsUrl)
    toast.success(t('settings.saved'))
    window.setTimeout(() => window.location.reload(), 700)
  }

  async function reloadAcl() {
    setAclBusy(true)
    try {
      const res = await api.reloadAcl()
      toast.success(t('settings.aclReloaded', { count: res.reloaded ?? 0 }))
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e))
    } finally {
      setAclBusy(false)
    }
  }

  const segBtn = (active: boolean) =>
    active ? 'bg-primary text-primary-foreground' : 'hover:bg-accent hover:text-accent-foreground'

  return (
    <div className="flex max-w-2xl flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>{t('settings.connection')}</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-5">
          <div className="flex flex-col gap-2">
            <Label htmlFor="set-api-base">{t('settings.apiBase')}</Label>
            <Input
              id="set-api-base"
              value={apiBase}
              onChange={(e) => setApiBaseState(e.target.value)}
              placeholder="/api/v1"
            />
            <p className="text-muted-foreground text-xs">{t('settings.apiBaseDesc')}</p>
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="set-token">{t('settings.token')}</Label>
            <Input
              id="set-token"
              type="password"
              value={token}
              onChange={(e) => setTokenState(e.target.value)}
              placeholder="••••••••"
              autoComplete="off"
            />
            <p className="text-muted-foreground text-xs">{t('settings.tokenDesc')}</p>
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="set-ws">{t('settings.wsUrl')}</Label>
            <Input
              id="set-ws"
              value={wsUrl}
              onChange={(e) => setWsUrlState(e.target.value)}
              placeholder="ws://localhost:8083/mqtt"
              className="font-mono"
            />
            <p className="text-muted-foreground text-xs">{t('settings.wsUrlDesc')}</p>
          </div>
          <div>
            <Button onClick={saveConnection}>
              <Save />
              {t('settings.save')}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>{t('settings.theme')}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col gap-5">
            <div className="flex flex-col gap-3">
              <div className="flex gap-1">
                {(['light', 'dark'] as const).map((v) => (
                  <Button
                    key={v}
                    size="sm"
                    variant={v === theme ? 'default' : 'outline'}
                    onClick={() => onThemeChange(v)}
                  >
                    {v === 'light' ? t('settings.light') : t('settings.dark')}
                  </Button>
                ))}
              </div>
            </div>

            <Separator />

            <div className="flex flex-col gap-3">
              <span className="text-sm font-medium">{t('settings.language')}</span>
              <div className="flex w-fit gap-1 rounded-md border p-1">
                {(['en', 'zh'] as const).map((v) => (
                  <button
                    key={v}
                    type="button"
                    onClick={() => changeLanguage(v)}
                    className={`rounded px-3 py-1.5 text-sm font-medium transition-colors ${segBtn(
                      lang === v,
                    )}`}
                  >
                    {v === 'en' ? 'English' : '简体中文'}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>{t('settings.acl')}</CardTitle>
          <CardDescription>{t('settings.aclDesc')}</CardDescription>
        </CardHeader>
        <CardContent>
          <Button variant="outline" onClick={() => void reloadAcl()} disabled={aclBusy}>
            <RefreshCw className={aclBusy ? 'animate-spin' : undefined} />
            {t('settings.reload')}
          </Button>
        </CardContent>
      </Card>
    </div>
  )
}
