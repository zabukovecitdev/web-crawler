import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'
import type { CrawlJobDto } from '@/types/api'

/** Number of Running crawl jobs, for the sidebar. Polled; the crawl feature has the live stream. */
export function useLiveCrawls(): number {
  const query = useQuery({
    queryKey: ['shell', 'live-crawls'],
    queryFn: () => apiFetch<CrawlJobDto[]>('/api/v1/crawl-jobs?limit=50'),
    refetchInterval: 10_000,
  })
  return query.data?.filter((job) => job.status === 'Running').length ?? 0
}
