import { useState, type FormEvent } from 'react'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { Send } from 'lucide-react'

import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export function PublishPage() {
  const { t } = useTranslation()
  const [topic, setTopic] = useState('')
  const [payload, setPayload] = useState('')
  const [qos, setQos] = useState(0)
  const [retain, setRetain] = useState(false)
  const [busy, setBusy] = useState(false)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    const trimmed = topic.trim()
    if (!trimmed) {
      toast.error(t('publish.needTopic'))
      return
    }
    setBusy(true)
    try {
      await api.publish({ topic: trimmed, payload, qos, retain })
      toast.success(t('publish.published', { topic: trimmed }))
      setPayload('')
      setRetain(false)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Card className="max-w-2xl">
      <CardHeader>
        <CardTitle>{t('publish.title')}</CardTitle>
      </CardHeader>
      <CardContent>
        <form className="flex flex-col gap-5" onSubmit={(e) => void onSubmit(e)}>
          <div className="flex flex-col gap-2">
            <Label htmlFor="pub-topic">{t('publish.topic')}</Label>
            <Input
              id="pub-topic"
              placeholder={t('publish.topicPh')}
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
            />
          </div>

          <div className="flex flex-col gap-2">
            <Label>{t('publish.qos')}</Label>
            <div className="flex w-fit gap-1 rounded-md border p-1">
              {[0, 1, 2].map((v) => (
                <Button
                  key={v}
                  type="button"
                  size="sm"
                  variant={qos === v ? 'default' : 'ghost'}
                  onClick={() => setQos(v)}
                >
                  {v}
                </Button>
              ))}
            </div>
          </div>

          <div className="flex flex-col gap-2">
            <Label htmlFor="pub-payload">{t('publish.payload')}</Label>
            <Textarea
              id="pub-payload"
              rows={6}
              placeholder={t('publish.payloadPh')}
              value={payload}
              onChange={(e) => setPayload(e.target.value)}
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              id="pub-retain"
              type="checkbox"
              checked={retain}
              onChange={(e) => setRetain(e.target.checked)}
              className="border-border bg-background data-[state=checked]:bg-primary size-4 rounded accent-primary"
            />
            <Label htmlFor="pub-retain">{t('publish.retain')}</Label>
          </div>

          <div className="flex justify-end gap-2">
            <Button type="submit" disabled={busy}>
              <Send />
              {t('publish.send')}
            </Button>
          </div>
        </form>
      </CardContent>
    </Card>
  )
}
