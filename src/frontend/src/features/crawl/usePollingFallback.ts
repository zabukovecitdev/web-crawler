import { useEffect, useRef } from 'react'
import { fetchEvents, fetchProgress } from './crawl-api'
import type { CrawlAction } from './crawl-state'

export const POLL_MS = 3000

async function pollOnce(jobId: number, after: number, dispatch: (action: CrawlAction) => void): Promise<number> {
  const [progress, events] = await Promise.all([fetchProgress(jobId), fetchEvents(jobId, after || undefined)])
  dispatch({ type: 'snapshot', progress })
  dispatch({ type: 'history', events })
  return events.reduce((max, event) => Math.max(max, event.id), after)
}

/** While the socket is down, re-reads /progress and /events?after=<lastId> every 3 s. */
export function usePollingFallback(jobId: number, enabled: boolean, dispatch: (action: CrawlAction) => void): void {
  const after = useRef(0)
  useEffect(() => {
    if (!enabled) {
      return undefined
    }
    const timer = setInterval(() => {
      pollOnce(jobId, after.current, dispatch).then((last) => {
        after.current = last
      }, () => undefined)
    }, POLL_MS)
    return () => clearInterval(timer)
  }, [jobId, enabled, dispatch])
}
