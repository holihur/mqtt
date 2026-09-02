import { useTranslation } from 'react-i18next'
import {
  Archive,
  HardDrive,
  LayoutDashboard,
  ListTree,
  Send,
  Settings,
  Users,
  type LucideIcon,
} from 'lucide-react'

import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

export type PageKey =
  | 'overview'
  | 'clients'
  | 'sessions'
  | 'subscriptions'
  | 'retained'
  | 'publish'
  | 'settings'

const NAV: { key: PageKey; icon: LucideIcon; labelKey: string }[] = [
  { key: 'overview', icon: LayoutDashboard, labelKey: 'nav.overview' },
  { key: 'clients', icon: Users, labelKey: 'nav.clients' },
  { key: 'sessions', icon: HardDrive, labelKey: 'nav.sessions' },
  { key: 'subscriptions', icon: ListTree, labelKey: 'nav.subscriptions' },
  { key: 'retained', icon: Archive, labelKey: 'nav.retained' },
  { key: 'publish', icon: Send, labelKey: 'nav.publish' },
  { key: 'settings', icon: Settings, labelKey: 'nav.settings' },
]

interface LayoutProps {
  current: PageKey
  onNavigate: (key: PageKey) => void
  headerRight?: React.ReactNode
  children: React.ReactNode
}

export function Layout({ current, onNavigate, headerRight, children }: LayoutProps) {
  const { t } = useTranslation()

  return (
    <div className="flex min-h-svh">
      <aside className="bg-sidebar text-sidebar-foreground sticky top-0 hidden h-svh w-60 shrink-0 flex-col border-r md:flex">
        <div className="flex h-14 items-center gap-2 border-b px-5">
          <div className="bg-primary text-primary-foreground flex size-7 items-center justify-center rounded-md text-xs font-bold">
            MQ
          </div>
          <div className="leading-tight">
            <div className="text-sm font-semibold">{t('app.title')}</div>
            <div className="text-muted-foreground text-xs">{t('app.subtitle')}</div>
          </div>
        </div>
        <nav className="flex flex-1 flex-col gap-1 p-3">
          {NAV.map(({ key, icon: Icon, labelKey }) => (
            <button
              key={key}
              onClick={() => onNavigate(key)}
              className={cn(
                'hover:bg-sidebar-accent hover:text-sidebar-accent-foreground flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                current === key && 'bg-sidebar-accent text-sidebar-accent-foreground',
              )}
            >
              <Icon className="size-4" />
              {t(labelKey)}
            </button>
          ))}
        </nav>
        <div className="text-muted-foreground border-t p-4 text-xs">admin API v1</div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="bg-background/95 supports-[backdrop-filter]:bg-background/60 sticky top-0 z-40 flex h-14 items-center gap-3 border-b px-4 backdrop-blur md:px-6">
          <div className="bg-primary text-primary-foreground flex size-7 shrink-0 items-center justify-center rounded-md text-xs font-bold md:hidden">
            MQ
          </div>
          <nav className="flex min-w-0 flex-1 gap-1 overflow-x-auto md:hidden">
            {NAV.map(({ key, icon: Icon, labelKey }) => (
              <Button
                key={key}
                variant={current === key ? 'secondary' : 'ghost'}
                size="icon"
                className="size-8 shrink-0"
                onClick={() => onNavigate(key)}
                title={t(labelKey)}
              >
                <Icon className="size-4" />
              </Button>
            ))}
          </nav>
          <div className="hidden min-w-0 flex-1 items-center gap-2 md:flex">
            <h1 className="truncate text-base font-semibold">
              {t(NAV.find((n) => n.key === current)?.labelKey ?? 'nav.overview')}
            </h1>
            <Badge variant="outline" className="hidden lg:inline-flex">
              /api/v1
            </Badge>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">{headerRight}</div>
        </header>
        <main className="flex-1 p-4 md:p-6">{children}</main>
      </div>
    </div>
  )
}
