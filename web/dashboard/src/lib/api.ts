const BASE_KEY = 'mqtt-dash.apiBase'
const TOKEN_KEY = 'mqtt-dash.token'
const LANG_KEY = 'mqtt-dash.lang'
const THEME_KEY = 'mqtt-dash.theme'

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

export function getTheme(): 'light' | 'dark' {
  const saved = localStorage.getItem(THEME_KEY)
  if (saved === 'light' || saved === 'dark') return saved
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export function setTheme(v: 'light' | 'dark') {
  localStorage.setItem(THEME_KEY, v)
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
  retained: (withPayload = false) =>
    request<RetainedMessage[]>(`/retained${withPayload ? '?with_payload=true' : ''}`),
  deleteRetained: (topic: string) =>
    request<OkResponse>(`/retained?topic=${encodeURIComponent(topic)}`, { method: 'DELETE' }),
  clearRetained: () => request<OkResponse>('/retained?all=true', { method: 'DELETE' }),
  publish: (body: PublishRequest) =>
    request<OkResponse>('/publish', { method: 'POST', body: JSON.stringify(body) }),
  reloadAcl: () => request<OkResponse>('/acl/reload', { method: 'POST' }),
}
