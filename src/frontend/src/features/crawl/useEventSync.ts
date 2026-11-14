import { useEffect, useRef } from 'react'
import { fetchEventsSince, fetchProgress } from './crawl-api'
import type { ConnectionState } from './crawl-hub'
import type { CrawlAction } from './crawl-state'

export const POLL_MS = 3000
export const RECONCILE_MS = 30_000

type Dispatch = (action: CrawlAction) => void

/** Every state but 'live' polls quickly; while live the page only reconciles now and then. */
export function syncInterval(connection: ConnectionState): number {
  return connection === 'live' ? RECONCILE_MS : POLL_MS
}

/** One sync: /progress and the events after the cursor, applied as history first, then the snapshot. */
export async function syncOnce(jobId: number, cursor: number, dispatch: Dispatch): Promise<void> {
  const [progress, events] = await Promise.all([fetchProgress(jobId), fetchEventsSince(jobId, cursor)])
  dispatch({ type: 'history', events })
  dispatch({ type: 'snapshot', progress })
}

/**
 * Keeps the page in step with the server: every 3 s while the socket is not live, every 30 s while it is,
 * and once on every transition into live (first connect, reconnect, recovery from offline).
 */
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
