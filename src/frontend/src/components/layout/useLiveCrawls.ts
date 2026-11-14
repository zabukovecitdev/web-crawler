import { useQuery } from '@tanstack/react-query'
import { crawlJobsKey, fetchCrawlJobs } from '@/lib/crawl-jobs'

/** Number of Running crawl jobs, for the sidebar. Polled; shares the crawl screens' jobs query. */
export function useLiveCrawls(): number {
  const query = useQuery({
    queryKey: crawlJobsKey,
    queryFn: fetchCrawlJobs,
    refetchInterval: 10_000,
  })
  return query.data?.filter((job) => job.status === 'Running').length ?? 0
}
