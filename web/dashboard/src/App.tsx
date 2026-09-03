import { useCallback, useEffect, useState, type ReactNode } from 'react'
import { Toaster } from 'sonner'

import { getTheme, setTheme } from '@/lib/api'
import { Layout, type PageKey } from '@/components/layout'
import { OverviewPage } from '@/pages/overview'
import { ClientsPage } from '@/pages/clients'
import { SessionsPage } from '@/pages/sessions'
import { SubscriptionsPage } from '@/pages/subscriptions'
import { RetainedPage } from '@/pages/retained'
import { LivePage } from '@/pages/live'
import { PublishPage } from '@/pages/publish'
import { SettingsPage } from '@/pages/settings'

const PAGES: PageKey[] = [
  'overview',
  'clients',
  'sessions',
  'subscriptions',
  'retained',
  'live',
  'publish',
  'settings',
]

function pageFromHash(): PageKey {
  const h = window.location.hash.replace(/^#\/?/, '')
  return (PAGES as string[]).includes(h) ? (h as PageKey) : 'overview'
}

function App() {
  const [page, setPage] = useState<PageKey>(pageFromHash)
  const [theme, setThemeState] = useState<'light' | 'dark'>(() => getTheme())

  useEffect(() => {
    const onHash = () => setPage(pageFromHash())
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark')
  }, [theme])

  const navigate = useCallback((key: PageKey) => {
    if (pageFromHash() !== key) window.location.hash = key
    setPage(key)
  }, [])

  const changeTheme = useCallback((v: 'light' | 'dark') => {
    setThemeState(v)
    setTheme(v)
  }, [])

  let content: ReactNode
  switch (page) {
    case 'overview':
      content = <OverviewPage />
      break
    case 'clients':
      content = <ClientsPage />
      break
    case 'sessions':
      content = <SessionsPage />
      break
    case 'subscriptions':
      content = <SubscriptionsPage />
      break
    case 'retained':
      content = <RetainedPage />
      break
    case 'live':
      content = <LivePage />
      break
    case 'publish':
      content = <PublishPage />
      break
    case 'settings':
      content = <SettingsPage theme={theme} onThemeChange={changeTheme} />
      break
  }

  return (
    <>
      <Layout current={page} onNavigate={navigate}>
        {content}
      </Layout>
      <Toaster richColors position="top-center" />
    </>
  )
}

export default App
