import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import mqtt, { type MqttClient } from 'mqtt'
import { ArrowDownToLine, Eraser, Plug, PlugZap } from 'lucide-react'

import { getMqttAuth, getWsUrl, setMqttAuth } from '@/lib/api'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

const MAX_MSGS = 500

type ConnState = 'idle' | 'connecting' | 'connected' | 'error' | 'closed'

interface LiveMsg {
  id: number
  at: number
  topic: string
  qos: number
  retain: boolean
  payload: string
  binary: boolean
}

function decodeBytes(bytes: Uint8Array): { text: string; binary: boolean } {
  try {
    return { text: new TextDecoder('utf-8', { fatal: true }).decode(bytes), binary: false }
  } catch {
    return {
      text: Array.from(bytes)
        .map((b) => b.toString(16).padStart(2, '0'))
        .join(' '),
      binary: true,
    }
  }
}

export function LivePage() {
  const { t } = useTranslation()
  const [filter, setFilter] = useState('#')
  const [username, setUsername] = useState(() => getMqttAuth().username)
  const [password, setPassword] = useState(() => getMqttAuth().password)
  const [state, setState] = useState<ConnState>('idle')
  const [subscribed, setSubscribed] = useState('')
  const [messages, setMessages] = useState<LiveMsg[]>([])
  const [autoScroll, setAutoScroll] = useState(true)
  const clientRef = useRef<MqttClient | null>(null)
  const idRef = useRef(0)
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    return () => {
      clientRef.current?.end(true)
    }
  }, [])

  useEffect(() => {
    if (autoScroll && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight
    }
  }, [messages, autoScroll])

  const pushMsg = useCallback(
    (topic: string, payload: Uint8Array, qos: number, retain: boolean) => {
      const { text, binary } = decodeBytes(payload)
      const id = ++idRef.current
      setMessages((cur) => {
        const next = [
          ...cur,
          { id, at: Date.now(), topic, qos, retain, payload: text, binary },
        ]
        return next.length > MAX_MSGS ? next.slice(next.length - MAX_MSGS) : next
      })
    },
    [],
  )

  const connect = useCallback(
    (topic: string) => {
      clientRef.current?.end(true)
      const auth = getMqttAuth()
      setState('connecting')
      setSubscribed('')
      const client = mqtt.connect(getWsUrl(), {
        clientId: `dash-${Math.random().toString(16).slice(2, 10)}`,
        clean: true,
        connectTimeout: 4000,
        reconnectPeriod: 0,
        protocolVersion: 4,
        username: auth.username || undefined,
        password: auth.password || undefined,
      })
      clientRef.current = client
      client.on('connect', () => {
        setState('connected')
        setSubscribed(topic)
        client.subscribe(topic, { qos: 0 }, (err) => {
          if (err) {
            setState('error')
            setSubscribed('')
          }
        })
      })
      client.on('message', (t, payload, packet) => {
        pushMsg(t, new Uint8Array(payload), packet.qos ?? 0, packet.retain ?? false)
      })
      client.on('error', () => {
        setState('error')
        setSubscribed('')
      })
      client.on('close', () => {
        setState('closed')
        setSubscribed('')
      })
    },
    [pushMsg],
  )

  function saveAuth() {
    setMqttAuth(username.trim(), password)
  }

  function onSubscribe() {
    const f = filter.trim()
    if (!f) return
    saveAuth()
    const c = clientRef.current
    if (c && c.connected) {
      if (subscribed) c.unsubscribe(subscribed)
      c.subscribe(f, { qos: 0 }, (err) => {
        if (err) {
          setState('error')
          setSubscribed('')
        } else {
          setSubscribed(f)
        }
      })
      return
    }
    connect(f)
  }

  function onDisconnect() {
    clientRef.current?.end(true)
    clientRef.current = null
    setState('idle')
    setSubscribed('')
  }

  function onClear() {
    setMessages([])
  }

  const connected = state === 'connected'
  const connecting = state === 'connecting'

  const statusBadge = () => {
    switch (state) {
      case 'connecting':
        return <Badge variant="warning">{t('live.statusConnecting')}</Badge>
      case 'connected':
        return <Badge variant="success">{t('live.statusConnected')}</Badge>
      case 'error':
        return <Badge variant="destructive">{t('live.statusError')}</Badge>
      case 'closed':
        return <Badge variant="secondary">{t('live.statusClosed')}</Badge>
      default:
        return <Badge variant="outline">{t('live.statusIdle')}</Badge>
    }
  }

  return (
    <div className="flex max-w-4xl flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>{t('live.title')}</CardTitle>
          <CardDescription>{t('live.desc')}</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-2">
            <Label htmlFor="live-topic">{t('live.topic')}</Label>
            <div className="flex gap-2">
              <Input
                id="live-topic"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder={t('live.topicPh')}
                className="font-mono"
              />
              {connected ? (
                <Button variant="outline" onClick={onDisconnect} disabled={connecting}>
                  <PlugZap />
                  {t('live.disconnect')}
                </Button>
              ) : (
                <Button onClick={onSubscribe} disabled={connecting || !filter.trim()}>
                  <Plug />
                  {t('live.subscribe')}
                </Button>
              )}
            </div>
            {connected && subscribed && (
              <p className="text-muted-foreground text-xs">
                {t('live.subscribedTo')}: <span className="font-mono">{subscribed}</span>
              </p>
            )}
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="flex flex-col gap-2">
              <Label htmlFor="live-user">{t('live.username')}</Label>
              <Input
                id="live-user"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoComplete="off"
              />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="live-pass">{t('live.password')}</Label>
              <Input
                id="live-pass"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="off"
              />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {statusBadge()}
            <span className="text-muted-foreground text-xs tabular-nums">
              {t('live.messageCount', { count: messages.length })}
            </span>
            <div className="ml-auto flex items-center gap-3">
              <label className="text-muted-foreground flex items-center gap-1.5 text-sm">
                <input
                  type="checkbox"
                  checked={autoScroll}
                  onChange={(e) => setAutoScroll(e.target.checked)}
                  className="size-4 rounded accent-primary"
                />
                <ArrowDownToLine className="size-4" />
                {t('live.autoScroll')}
              </label>
              <Button variant="outline" size="sm" onClick={onClear}>
                <Eraser />
                {t('live.clear')}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="p-0">
          <div ref={listRef} className="max-h-[60vh] overflow-auto">
            {messages.length === 0 ? (
              <div className="text-muted-foreground flex h-40 items-center justify-center text-sm">
                {t('live.empty')}
              </div>
            ) : (
              <ul className="divide-y">
                {messages.map((m) => (
                  <li key={m.id} className="px-4 py-2 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-muted-foreground text-xs tabular-nums">
                        {new Date(m.at).toLocaleTimeString([], { hour12: false })}
                      </span>
                      <span className="font-mono font-medium break-all">{m.topic}</span>
                      <Badge variant="secondary">QoS {m.qos}</Badge>
                      {m.retain && <Badge variant="warning">{t('live.retain')}</Badge>}
                      {m.binary && <Badge variant="outline">{t('live.binary')}</Badge>}
                    </div>
                    <pre className="text-muted-foreground mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-all font-mono text-xs">
                      {m.payload}
                    </pre>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
