import { useEffect, useState } from 'react'
import { notifyUnauthorized } from '@/lib/api'
import type { LiveEvent } from './crawl-api'
import { connectCrawlHub, type ConnectionState } from './crawl-hub'

const useMocks = import.meta.env.VITE_USE_MOCKS === 'true'

function connectMock(
  jobId: number,
  onEvent: (event: LiveEvent) => void,
  onState: (state: ConnectionState) => void,
): () => void {
  let cancelled = false
  let close = () => {}
  void import('@/mocks/crawlStream').then(({ createMockCrawlStream }) => {
    if (cancelled) {
      return
    }
    const stream = createMockCrawlStream(jobId)
    stream.onState(onState)
    stream.subscribe(onEvent)
    close = stream.close
    onState('live')
  })
  return () => {
    cancelled = true
    close()
  }
}

export function useCrawlStream(
  jobId: number,
  onEvent: (event: LiveEvent) => void,
  enabled: boolean = true,
): ConnectionState {
  const [state, setState] = useState<ConnectionState>('connecting')

  useEffect(() => {
    if (!enabled) {
      return undefined
    }
    if (useMocks) {
      return connectMock(jobId, onEvent, setState)
    }
    return connectCrawlHub(jobId, { onEvent, onState: setState, onUnauthorized: notifyUnauthorized })
  }, [jobId, enabled, onEvent])

  return enabled ? state : 'offline'
}
