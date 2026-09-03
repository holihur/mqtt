import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import mqtt, { type MqttClient } from 'mqtt'
import { ArrowDownToLine, Eraser, Plug, PlugZap, Plus, X } from 'lucide-react'

import { discoverWsUrl, getMqttAuth, setMqttAuth } from '@/lib/api'
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
import { Textarea } from '@/components/ui/textarea'

const MAX_MSGS = 500
const SELECT_CLASS =
  'border-input placeholder:text-muted-foreground dark:bg-input/30 focus-visible:border-ring focus-visible:ring-ring/50 flex h-9 w-full min-w-0 rounded-md border bg-transparent px-3 py-1 text-sm shadow-xs transition-[color,box-shadow] outline-none focus-visible:ring-[3px]'

type ConnState = 'idle' | 'connecting' | 'connected' | 'error' | 'closed'
type QoS = 0 | 1 | 2

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
  const [topics, setTopics] = useState<string[]>([])
  const [draft, setDraft] = useState('')
  const [subscribed, setSubscribed] = useState<string[]>([])
  const [username, setUsername] = useState(() => getMqttAuth().username)
  const [password, setPassword] = useState(() => getMqttAuth().password)
  const [willEnabled, setWillEnabled] = useState(false)
  const [willTopic, setWillTopic] = useState('')
  const [willPayload, setWillPayload] = useState('')
  const [willQos, setWillQos] = useState<QoS>(0)
  const [willRetain, setWillRetain] = useState(false)
  const [protocolVersion, setProtocolVersion] = useState<3 | 4 | 5>(4)
  const [state, setState] = useState<ConnState>('idle')
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
    (wsUrl: string, topicList: string[]) => {
      clientRef.current?.end(true)
      const auth = getMqttAuth()
      setState('connecting')
      setSubscribed([])
      const client = mqtt.connect(wsUrl, {
        clientId: `dash-${Math.random().toString(16).slice(2, 10)}`,
        clean: true,
        connectTimeout: 4000,
        reconnectPeriod: 0,
        protocolVersion,
        username: auth.username || undefined,
        password: auth.password || undefined,
        will:
          willEnabled && willTopic.trim()
            ? {
                topic: willTopic.trim(),
                payload: willPayload,
                qos: willQos,
                retain: willRetain,
              }
            : undefined,
      })
      clientRef.current = client
      client.on('connect', () => {
        setState('connected')
        client.subscribe(topicList, { qos: 0 }, (err) => {
          if (err) {
            setState('error')
            setSubscribed([])
          } else {
            setSubscribed(topicList)
          }
        })
      })
      client.on('message', (t, payload, packet) => {
        pushMsg(t, new Uint8Array(payload), packet.qos ?? 0, packet.retain ?? false)
      })
      client.on('error', () => {
        setState('error')
        setSubscribed([])
      })
      client.on('close', () => {
        setState('closed')
        setSubscribed([])
      })
    },
    [pushMsg, willEnabled, willTopic, willPayload, willQos, willRetain, protocolVersion],
  )

  function saveAuth() {
    setMqttAuth(username.trim(), password)
  }

  function addTopic(raw?: string) {
    const value = (raw ?? draft).trim()
    if (!value) return
    setTopics((cur) => (cur.includes(value) ? cur : [...cur, value]))
    setDraft('')
    const c = clientRef.current
    if (c && c.connected) {
      c.subscribe(value, { qos: 0 }, () => {
        setSubscribed((cur) => (cur.includes(value) ? cur : [...cur, value]))
      })
    }
  }

  function removeTopic(topic: string) {
    setTopics((cur) => cur.filter((x) => x !== topic))
    setSubscribed((cur) => cur.filter((x) => x !== topic))
    const c = clientRef.current
    if (c && c.connected) c.unsubscribe(topic)
  }

  async function onSubscribe() {
    if (topics.length === 0) return
    saveAuth()
    setState('connecting')
    const wsUrl = await discoverWsUrl()
    connect(wsUrl, topics)
  }

  function onDisconnect() {
    clientRef.current?.end(true)
    clientRef.current = null
    setState('idle')
    setSubscribed([])
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
            <Label htmlFor="live-topic">{t('live.topics')}</Label>
            <div className="flex gap-2">
              <Input
                id="live-topic"
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') addTopic()
                }}
                placeholder={t('live.topicPh')}
                className="font-mono"
              />
              <Button variant="outline" onClick={() => addTopic()} disabled={!draft.trim()}>
                <Plus />
                {t('live.addTopic')}
              </Button>
            </div>
            {topics.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {topics.map((topic) => (
                  <Badge
                    key={topic}
                    variant={subscribed.includes(topic) ? 'success' : 'secondary'}
                    className="gap-1"
                  >
                    <span className="font-mono">{topic}</span>
                    <button
                      type="button"
                      onClick={() => removeTopic(topic)}
                      className="text-muted-foreground hover:text-foreground inline-flex size-3.5 items-center justify-center rounded-sm"
                      aria-label={`${t('common.delete')} ${topic}`}
                    >
                      <X className="size-3" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}
            <div className="flex gap-2">
              {connected ? (
                <Button variant="outline" onClick={onDisconnect} disabled={connecting}>
                  <PlugZap />
                  {t('live.disconnect')}
                </Button>
              ) : (
                <Button onClick={onSubscribe} disabled={connecting || topics.length === 0}>
                  <Plug />
                  {t('live.subscribe')}
                </Button>
              )}
            </div>
            {connected && subscribed.length > 0 && (
              <p className="text-muted-foreground text-xs">
                {t('live.subscribedTo')}: <span className="font-mono">{subscribed.join(', ')}</span>
              </p>
            )}
          </div>

          <div className="grid gap-4 sm:grid-cols-3">
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
            <div className="flex flex-col gap-2">
              <Label htmlFor="live-proto">{t('live.protocolVersion')}</Label>
              <select
                id="live-proto"
                value={protocolVersion}
                onChange={(e) => setProtocolVersion(Number(e.target.value) as 3 | 4 | 5)}
                className={SELECT_CLASS}
              >
                <option value={3}>MQTT 3.1</option>
                <option value={4}>MQTT 3.1.1</option>
                <option value={5}>MQTT 5.0</option>
              </select>
            </div>
          </div>

          <div className="flex flex-col gap-3 rounded-md border p-3">
            <label className="flex cursor-pointer items-center gap-2 text-sm font-medium">
              <input
                type="checkbox"
                checked={willEnabled}
                onChange={(e) => setWillEnabled(e.target.checked)}
                className="size-4 rounded accent-primary"
              />
              {t('live.willEnable')}
            </label>
            <p className="text-muted-foreground text-xs">{t('live.willDesc')}</p>
            {willEnabled && (
              <>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="live-will-topic">{t('live.willTopic')}</Label>
                  <Input
                    id="live-will-topic"
                    value={willTopic}
                    onChange={(e) => setWillTopic(e.target.value)}
                    placeholder={t('live.willTopicPh')}
                    className="font-mono"
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="live-will-payload">{t('live.willPayload')}</Label>
                  <Textarea
                    id="live-will-payload"
                    value={willPayload}
                    onChange={(e) => setWillPayload(e.target.value)}
                    className="font-mono"
                  />
                </div>
                <div className="grid grid-cols-2 items-end gap-4 sm:grid-cols-3">
                  <div className="flex flex-col gap-2">
                    <Label htmlFor="live-will-qos">{t('live.willQos')}</Label>
                    <select
                      id="live-will-qos"
                      value={willQos}
                      onChange={(e) => setWillQos(Number(e.target.value) as QoS)}
                      className={SELECT_CLASS}
                    >
                      <option value={0}>QoS 0</option>
                      <option value={1}>QoS 1</option>
                      <option value={2}>QoS 2</option>
                    </select>
                  </div>
                  <label className="flex items-center gap-2 pb-2 text-sm">
                    <input
                      type="checkbox"
                      checked={willRetain}
                      onChange={(e) => setWillRetain(e.target.checked)}
                      className="size-4 rounded accent-primary"
                    />
                    {t('live.willRetain')}
                  </label>
                </div>
              </>
            )}
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
