import { useEffect, useState } from 'react'

/** Date.now(), refreshed every `intervalMs` while `active`. For tickers such as "sent 0.4 s ago". */
export function useNow(active: boolean, intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!active) {
      return undefined
    }
    const timer = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(timer)
  }, [active, intervalMs])
  return now
}
