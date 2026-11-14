import { useQuery } from '@tanstack/react-query'
import { useCallback, useEffect, useReducer, type Dispatch } from 'react'
import type { CrawlEvent } from '@/types/api'
import { crawlKeys, fetchEvents, fetchJob, fetchProgress } from './crawl-api'
import { crawlReducer, initialCrawlState, isFinished, type CrawlAction } from './crawl-state'
import { useCrawlStream } from './useCrawlStream'
import { usePollingFallback } from './usePollingFallback'

const NO_REFETCH = { refetchOnWindowFocus: false } as const

/** Loads /progress and /events once and feeds them to the reducer. */
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

/** Initial state, then live events (socket) or polling (offline). Stops once the job has ended. */
export function useCrawlData(jobId: number) {
  const [state, dispatch] = useReducer(crawlReducer, initialCrawlState)
  const job = useQuery({ queryKey: crawlKeys.job(jobId), queryFn: () => fetchJob(jobId) })
  const progress = useInitialLoad(jobId, dispatch)

  const finished = isFinished(state.progress?.status)
  const active = progress.isSuccess && !finished
  const onEvent = useCallback((event: CrawlEvent) => dispatch({ type: 'live', event }), [])
  const connection = useCrawlStream(jobId, onEvent, active)
  usePollingFallback(jobId, active && connection === 'offline', dispatch)

  const refetchJob = job.refetch
  useEffect(() => {
    if (finished) {
      void refetchJob()
    }
  }, [finished, refetchJob])

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
