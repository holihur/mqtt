const BASE_KEY = 'mqtt-dash.apiBase'
const TOKEN_KEY = 'mqtt-dash.token'
const LANG_KEY = 'mqtt-dash.lang'
const THEME_KEY = 'mqtt-dash.theme'
const WS_KEY = 'mqtt-dash.wsUrl'
const MQTT_USER_KEY = 'mqtt-dash.mqttUser'
const MQTT_PASS_KEY = 'mqtt-dash.mqttPass'
const SIDEBAR_KEY = 'mqtt-dash.sidebarCollapsed'

let cachedInfoWsUrl: string | null = null

export function getApiBase(): string {
  const saved = localStorage.getItem(BASE_KEY)
  if (saved) return saved
  const env = (import.meta.env.VITE_API_BASE as string | undefined) ?? '/api/v1'
  return env.replace(/\/$/, '')
}

export function setApiBase(v: string) {
  const v2 = v.trim().replace(/\/$/, '')
  if (v2) localStorage.setItem(BASE_KEY, v2)
  else localStorage.removeItem(BASE_KEY)
}

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) ?? ''
}

export function setToken(v: string) {
  if (v) localStorage.setItem(TOKEN_KEY, v)
  else localStorage.removeItem(TOKEN_KEY)
}

export function getLang(): string {
  return localStorage.getItem(LANG_KEY) ?? (navigator.language.toLowerCase().startsWith('zh') ? 'zh' : 'en')
}

export function setLang(v: string) {
  localStorage.setItem(LANG_KEY, v)
}

export function getSidebarCollapsed(): boolean {
  return localStorage.getItem(SIDEBAR_KEY) === '1'
}

export function setSidebarCollapsed(v: boolean) {
  if (v) localStorage.setItem(SIDEBAR_KEY, '1')
  else localStorage.removeItem(SIDEBAR_KEY)
}

export function getTheme(): 'light' | 'dark' {
  const saved = localStorage.getItem(THEME_KEY)
  if (saved === 'light' || saved === 'dark') return saved
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export function setTheme(v: 'light' | 'dark') {
  localStorage.setItem(THEME_KEY, v)
}

// WebSocket (MQTT over WS) 地址：优先手动配置，其次由 /info 自动发现，最后回退默认。
export function getWsUrl(): string {
  const saved = localStorage.getItem(WS_KEY)
  if (saved) return saved
  return cachedInfoWsUrl ?? defaultWsUrl()
}

export function defaultWsUrl(): string {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${proto}://${window.location.hostname}:8083/mqtt`
}

// 把 broker 配置的监听地址（如 ":8083" / "0.0.0.0:8083" / "127.0.0.1:8083"）
// 解析成浏览器可用的 ws:// URL。
export function resolveWsUrl(addr?: string): string | null {
  if (!addr) return null
  let host = addr.trim()
  let port = ''
  if (host.startsWith('[')) {
    const end = host.indexOf(']')
    if (end < 0) return null
    const rest = host.slice(end + 1)
    host = host.slice(0, end + 1)
    if (rest.startsWith(':')) port = rest.slice(1)
  } else {
    const idx = host.lastIndexOf(':')
    if (idx >= 0) {
      const maybePort = host.slice(idx + 1)
      if (/^\d+$/.test(maybePort)) {
        port = maybePort
        host = host.slice(0, idx)
      }
    }
  }
  if (!port) port = '8083'
  // 监听在通配/空地址时，用浏览器当前主机名回填。
  if (host === '' || host === '0.0.0.0' || host === '[::]' || host === '::') {
    host = window.location.hostname
  }
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${proto}://${host}:${port}/mqtt`
}

export function setWsUrl(v: string) {
  const v2 = v.trim().replace(/\/$/, '')
  if (v2) localStorage.setItem(WS_KEY, v2)
  else localStorage.removeItem(WS_KEY)
}

// MQTT (WS) 连接可选的用户名/密码，用于 SimpleAuth / FileACL / DB auth 等场景。
export function getMqttAuth(): { username: string; password: string } {
  return {
    username: localStorage.getItem(MQTT_USER_KEY) ?? '',
    password: localStorage.getItem(MQTT_PASS_KEY) ?? '',
  }
}

export function setMqttAuth(username: string, password: string) {
  if (username) localStorage.setItem(MQTT_USER_KEY, username)
  else localStorage.removeItem(MQTT_USER_KEY)
  if (password) localStorage.setItem(MQTT_PASS_KEY, password)
  else localStorage.removeItem(MQTT_PASS_KEY)
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  headers.set('Accept', 'application/json')
  if (init?.body) headers.set('Content-Type', 'application/json')
  const token = getToken()
  if (token) headers.set('Authorization', `Bearer ${token}`)

  const res = await fetch(`${getApiBase()}${path}`, { ...init, headers })
  if (!res.ok) {
    let msg = `HTTP ${res.status}`
    try {
      const data = (await res.json()) as { error?: string }
      if (data?.error) msg = data.error
    } catch {
      // ignore body parse errors
    }
    throw new Error(msg)
  }
  return (await res.json()) as T
}

export interface Info {
  nodeId: string
  version: string
  commit: string
  date: string
  uptimeSeconds: number
  mode: 'cluster' | 'standalone'
  redisAddr: string
  adminEnabled: boolean
  adminTls: boolean
  wsAddr?: string
}

export interface Stats {
  startedAt: string
  uptimeSeconds: number
  messagesReceived: number
  messagesSent: number
  clientsConnected: number
  clientsTotal: number
  sessions: number
  retainedMessages: number
  retainedSizeBytes: number
  nodes: string[]
}

export interface Health {
  status: string
}

export interface ClientInfo {
  clientId: string
  username: string
  version: string
  remoteAddr: string
  keepAlive: number
  cleanStart: boolean
  sessionExpiry: number
  nodeId: string
  subscriptions: number
  inflight: number
  connectedAt: string
}

export interface SessionInfo {
  clientId: string
  username: string
  version: string
  connected: boolean
  cleanStart: boolean
  sessionExpiry: number
  keepAlive: number
  createdAt: string
  nodeId: string
  subscriptions: number
  inflight: number
}

export interface Subscription {
  clientId: string
  filter: string
  qos: number
  noLocal: boolean
}

export interface RetainedMessage {
  topic: string
  qos: number
  size: number
  payloadB64?: string
}

export interface PublishRequest {
  topic: string
  payload?: string
  payloadB64?: string
  qos: number
  retain: boolean
}

export interface OkResponse {
  ok: boolean
  topic?: string
  clientId?: string
  deleted?: number
  reloaded?: number
}

export const api = {
  info: () => request<Info>('/info'),
  stats: () => request<Stats>('/stats'),
  health: () => request<Health>('/health'),
  healthOk: async () => {
    try {
      const h = await request<Health>('/health')
      return h.status === 'ok'
    } catch {
      return false
    }
  },
  nodes: () => request<{ nodes: string[] }>('/nodes'),
  clients: () => request<ClientInfo[]>('/clients'),
  kickClient: (clientId: string) =>
    request<OkResponse>(`/clients/${encodeURIComponent(clientId)}`, { method: 'DELETE' }),
  sessions: () => request<SessionInfo[]>('/sessions'),
  deleteSession: (clientId: string) =>
    request<OkResponse>(`/sessions/${encodeURIComponent(clientId)}`, { method: 'DELETE' }),
  subscriptions: (clientId?: string) =>
    request<Subscription[]>(
      clientId
        ? `/subscriptions/${encodeURIComponent(clientId)}`
        : '/subscriptions',
    ),
  matchSubscriptions: (topic: string) =>
    request<Subscription[]>(`/subscriptions/match?topic=${encodeURIComponent(topic)}`),
  retained: (withPayload = false) =>
    request<RetainedMessage[]>(`/retained${withPayload ? '?with_payload=true' : ''}`),
  deleteRetained: (topic: string) =>
    request<OkResponse>(`/retained?topic=${encodeURIComponent(topic)}`, { method: 'DELETE' }),
  clearRetained: () => request<OkResponse>('/retained?all=true', { method: 'DELETE' }),
  publish: (body: PublishRequest) =>
    request<OkResponse>('/publish', { method: 'POST', body: JSON.stringify(body) }),
  reloadAcl: () => request<OkResponse>('/acl/reload', { method: 'POST' }),
}

// 异步发现 WS 地址：从 /api/v1/info 读取 wsAddr 并缓存；失败时回退默认。
export async function discoverWsUrl(): Promise<string> {
  const saved = localStorage.getItem(WS_KEY)
  if (saved) return saved
  if (cachedInfoWsUrl) return cachedInfoWsUrl
  try {
    const info = await api.info()
    const resolved = resolveWsUrl(info.wsAddr)
    if (resolved) {
      cachedInfoWsUrl = resolved
      return resolved
    }
  } catch {
    // 忽略发现失败，走默认地址
  }
  return defaultWsUrl()
}
