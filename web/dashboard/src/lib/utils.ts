import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let v = bytes / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(1)} ${units[i]}`
}

export function formatNumber(n: number): string {
  return new Intl.NumberFormat().format(n)
}

export function formatUptime(seconds: number): string {
  const d = Math.floor(seconds / 86400)
  const h = Math.floor((seconds % 86400) / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  if (d > 0) return `${d}d ${h}h ${m}m`
  if (h > 0) return `${h}h ${m}m`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

export function formatTime(ts: string | number): string {
  return new Date(ts).toLocaleString()
}

export type DecodedPayload =
  | { kind: 'text'; value: string }
  | { kind: 'hex'; value: string }

export function decodePayload(b64: string): DecodedPayload {
  let bytes: Uint8Array
  try {
    const bin = atob(b64)
    bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0))
  } catch {
    bytes = new Uint8Array(0)
  }
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    return { kind: 'text', value: text }
  } catch {
    const value = Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join(' ')
    return { kind: 'hex', value }
  }
}
