import { useEffect, useState } from 'react'
import { createMockCrawlStream } from '@/mocks/crawlStream'
import type { CrawlEvent } from '@/types/api'
import { connectCrawlHub, type ConnectionState } from './crawl-hub'

const useMocks = import.meta.env.VITE_USE_MOCKS === 'true'

/**
 * Live CrawlEvents for one job. SignalR against the API, or the in-memory stream when mocks are on.
 * Returns the connection state; while it is not 'live' the page polls instead.
 */
export function useCrawlStream(
  jobId: number,
  onEvent: (event: CrawlEvent) => void,
  enabled: boolean = true,
): ConnectionState {
  const [state, setState] = useState<ConnectionState>(useMocks ? 'live' : 'reconnecting')

  useEffect(() => {
    if (!enabled) {
      return undefined
    }
    if (useMocks) {
      const stream = createMockCrawlStream(jobId)
      stream.subscribe(onEvent)
      return stream.close
    }
    return connectCrawlHub(jobId, { onEvent, onState: setState })
  }, [jobId, enabled, onEvent])

  return enabled ? state : 'offline'
}
