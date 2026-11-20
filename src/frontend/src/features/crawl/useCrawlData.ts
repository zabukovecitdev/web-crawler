import { useQuery } from '@tanstack/react-query'
import { useCallback, useEffect, useReducer, useRef, type Dispatch } from 'react'
import type { CrawlEvent } from '@/types/api'
import { crawlKeys, fetchEvents, fetchJob, fetchProgress } from './crawl-api'
import { pendingLinks } from './link-format'
import { crawlReducer, initialCrawlState, isFinished, type CrawlAction } from './crawl-state'
import { useCrawlStream } from './useCrawlStream'
import { syncOnce, useEventSync } from './useEventSync'

const NO_REFETCH = { refetchOnWindowFocus: false } as const

function useInitialLoad(jobId: number, dispatch: Dispatch<CrawlAction>) {
  const progress = useQuery({ queryKey: crawlKeys.progress(jobId), queryFn: () => fetchProgress(jobId), ...NO_REFETCH })
  const events = useQuery({ queryKey: crawlKeys.events(jobId), queryFn: () => fetchEvents(jobId), ...NO_REFETCH })
  useEffect(() => {
    if (progress.data) {
      dispatch({ type: 'snapshot', progress: progress.data })
    }
  }, [progress.data, dispatch])
  useEffect(() => {
    if (events.data) {
      dispatch({ type: 'history', events: events.data })
    }
  }, [events.data, dispatch])
  return progress
}

function useJob(jobId: number) {
  return useQuery({
    queryKey: crawlKeys.job(jobId),
    queryFn: () => fetchJob(jobId),
    refetchInterval: ({ state }) => (state.data && isFinished(state.data.status) && pendingLinks(state.data.links) === 0 ? false : 4000),
  })
}

export function useCrawlData(jobId: number) {
  const [state, dispatch] = useReducer(crawlReducer, initialCrawlState)
  const job = useJob(jobId)
  const progress = useInitialLoad(jobId, dispatch)

  const lastEventId = useRef(0)
  useEffect(() => {
    lastEventId.current = state.lastEventId
  }, [state.lastEventId])
  const finished = isFinished(state.progress?.status)
  const active = progress.isSuccess && !finished
  const onEvent = useCallback((event: CrawlEvent) => dispatch({ type: 'live', event }), [])
  const connection = useCrawlStream(jobId, onEvent, active)
  useEventSync(jobId, active, connection, state.lastEventId, dispatch)

  const refetchJob = job.refetch
  useEffect(() => {
    if (finished) {
      void refetchJob()
      syncOnce(jobId, lastEventId.current, dispatch).catch(() => undefined)
    }
  }, [finished, refetchJob, jobId])

  return {
    state,
    job,
    finished,
    connection,
    loading: progress.isPending || job.isPending,
    error: progress.error ?? job.error,
    retry: () => void progress.refetch(),
  }
}
