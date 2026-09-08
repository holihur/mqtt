import { useEffect, useState } from 'react'

/**
 * useState that persists to localStorage, so page state survives
 * switching pages (component unmount) and full reloads.
 */
export function usePersistedState<T>(key: string, initial: T) {
  const storageKey = `dash:${key}`
  const [value, setValue] = useState<T>(() => {
    try {
      const raw = localStorage.getItem(storageKey)
      if (raw !== null) return JSON.parse(raw) as T
    } catch {
      // corrupted entry — fall back to initial
    }
    return initial
  })

  useEffect(() => {
    try {
      localStorage.setItem(storageKey, JSON.stringify(value))
    } catch {
      // storage full or unavailable — non-fatal
    }
  }, [storageKey, value])

  return [value, setValue] as const
}
