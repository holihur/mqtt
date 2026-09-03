import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Archive,
  HardDrive,
  LayoutDashboard,
  ListTree,
  PanelLeftClose,
  PanelLeftOpen,
  Radio,
  Send,
  Settings,
  Users,
  type LucideIcon,
} from 'lucide-react'

import { getSidebarCollapsed, setSidebarCollapsed } from '@/lib/api'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

export type PageKey =
  | 'overview'
  | 'clients'
  | 'sessions'
  | 'subscriptions'
  | 'retained'
  | 'live'
  | 'publish'
  | 'settings'

const NAV: { key: PageKey; icon: LucideIcon; labelKey: string }[] = [
  { key: 'overview', icon: LayoutDashboard, labelKey: 'nav.overview' },
  { key: 'clients', icon: Users, labelKey: 'nav.clients' },
  { key: 'sessions', icon: HardDrive, labelKey: 'nav.sessions' },
  { key: 'subscriptions', icon: ListTree, labelKey: 'nav.subscriptions' },
  { key: 'retained', icon: Archive, labelKey: 'nav.retained' },
  { key: 'live', icon: Radio, labelKey: 'nav.live' },
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
  const [collapsed, setCollapsed] = useState(() => getSidebarCollapsed())

  const toggleCollapsed = () => {
    const next = !collapsed
    setCollapsed(next)
    setSidebarCollapsed(next)
  }

  return (
    <div className="flex min-h-svh">
      <aside
        className={cn(
          'bg-sidebar text-sidebar-foreground sticky top-0 hidden h-svh shrink-0 flex-col border-r transition-[width] duration-200 md:flex',
          collapsed ? 'w-16' : 'w-60',
        )}
      >
        <div
          className={cn(
            'flex h-14 items-center border-b',
            collapsed ? 'justify-center px-2' : 'gap-2 px-4',
          )}
        >
          {collapsed ? (
            <Button
              variant="ghost"
              size="icon"
              className="size-8"
              onClick={toggleCollapsed}
              title={t('nav.expand')}
            >
              <PanelLeftOpen className="size-4" />
            </Button>
          ) : (
            <>
              <div className="bg-primary text-primary-foreground flex size-7 shrink-0 items-center justify-center rounded-md text-xs font-bold">
                MQ
              </div>
              <div className="min-w-0 leading-tight">
                <div className="truncate text-sm font-semibold">{t('app.title')}</div>
                <div className="text-muted-foreground truncate text-xs">{t('app.subtitle')}</div>
              </div>
              <Button
                variant="ghost"
                size="icon"
                className="ml-auto size-8"
                onClick={toggleCollapsed}
                title={t('nav.collapse')}
              >
                <PanelLeftClose className="size-4" />
              </Button>
            </>
          )}
        </div>
        <nav className={cn('flex flex-1 flex-col gap-1', collapsed ? 'items-center p-2' : 'p-3')}>
          {NAV.map(({ key, icon: Icon, labelKey }) => (
            <button
              key={key}
              onClick={() => onNavigate(key)}
              title={collapsed ? t(labelKey) : undefined}
              className={cn(
                'hover:bg-sidebar-accent hover:text-sidebar-accent-foreground flex items-center gap-3 rounded-md text-sm font-medium transition-colors',
                collapsed ? 'size-10 justify-center p-0' : 'px-3 py-2',
                current === key && 'bg-sidebar-accent text-sidebar-accent-foreground',
              )}
            >
              <Icon className="size-4 shrink-0" />
              {!collapsed && t(labelKey)}
            </button>
          ))}
        </nav>
        {!collapsed && (
          <div className="text-muted-foreground border-t p-4 text-xs">admin API v1</div>
        )}
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
