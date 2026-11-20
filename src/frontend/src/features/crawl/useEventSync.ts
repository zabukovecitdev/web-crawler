import { useEffect, useRef } from 'react'
import { fetchEventsSince, fetchProgress } from './crawl-api'
import type { ConnectionState } from './crawl-hub'
import type { CrawlAction } from './crawl-state'

export const POLL_MS = 3000
export const RECONCILE_MS = 30_000

type Dispatch = (action: CrawlAction) => void

export function syncInterval(connection: ConnectionState): number {
  return connection === 'live' ? RECONCILE_MS : POLL_MS
}

export async function syncOnce(jobId: number, cursor: number, dispatch: Dispatch): Promise<void> {
  const [progress, events] = await Promise.all([fetchProgress(jobId), fetchEventsSince(jobId, cursor)])
  dispatch({ type: 'history', events })
  dispatch({ type: 'snapshot', progress })
}

export function useEventSync(
  jobId: number,
  active: boolean,
  connection: ConnectionState,
  lastEventId: number,
  dispatch: Dispatch,
): void {
  const cursor = useRef(lastEventId)
  useEffect(() => {
    cursor.current = lastEventId
  }, [lastEventId])

  useEffect(() => {
    if (!active) {
      return undefined
    }
    const run = () => {
      syncOnce(jobId, cursor.current, dispatch).catch(() => undefined)
    }
    if (connection === 'live') {
      run()
    }
    const timer = setInterval(run, syncInterval(connection))
    return () => clearInterval(timer)
  }, [jobId, active, connection, dispatch])
}
