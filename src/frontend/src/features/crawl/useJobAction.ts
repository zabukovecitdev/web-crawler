import { useMutation, useQueryClient } from '@tanstack/react-query'
import type { CrawlJobAction } from '@/types/api'
import { crawlKeys, sendAction } from './crawl-api'

/** Sends a job action (pause, retry-failed, recrawl, ...) and refreshes everything that shows the job. */
export function useJobAction(jobId: number) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (action: CrawlJobAction) => sendAction(jobId, action),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.job(jobId) })
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
    },
  })
}
