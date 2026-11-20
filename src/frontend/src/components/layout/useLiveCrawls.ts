import { useQuery } from '@tanstack/react-query'
import { crawlJobsKey, fetchCrawlJobs } from '@/lib/crawl-jobs'

export function useLiveCrawls(): number {
  const query = useQuery({
    queryKey: crawlJobsKey,
    queryFn: fetchCrawlJobs,
    refetchInterval: 10_000,
  })
  return query.data?.filter((job) => job.status === 'Running').length ?? 0
}
