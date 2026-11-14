import { useQuery, type UseQueryResult } from '@tanstack/react-query'
import { useState } from 'react'
import { PageHeader } from '@/components/shared/PageHeader'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import type { CrawlJobDto } from '@/types/api'
import { CrawlsTable } from './CrawlsTable'
import { RequestCrawlDialog } from './RequestCrawlDialog'
import { crawlKeys, fetchJobs } from './crawl-api'

function JobsBody({ jobs }: { jobs: UseQueryResult<CrawlJobDto[]> }) {
  if (jobs.isError) {
    return <ErrorState error={jobs.error} onRetry={() => void jobs.refetch()} />
  }
  if (!jobs.data) {
    return <LoadingRows />
  }
  if (jobs.data.length === 0) {
    return <EmptyState title="No crawls yet">Request a crawl to give SamoBot a site to read.</EmptyState>
  }
  return <CrawlsTable jobs={jobs.data} />
}

function kickerOf(jobs: CrawlJobDto[] | undefined): string {
  if (!jobs) {
    return 'Crawls'
  }
  const live = jobs.filter((job) => job.status === 'Running').length
  return `Crawls · ${jobs.length}${live ? ` · ${live} live` : ''}`
}

export function CrawlsPage() {
  const [requesting, setRequesting] = useState(false)
  const jobs = useQuery({ queryKey: crawlKeys.jobs, queryFn: fetchJobs, refetchInterval: 10_000 })

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        kicker={kickerOf(jobs.data)}
        title="Crawls"
        actions={<Button onClick={() => setRequesting(true)}>Request crawl</Button>}
      />
      <Card className="p-5 shadow-zb-1">
        <JobsBody jobs={jobs} />
      </Card>
      <RequestCrawlDialog open={requesting} onOpenChange={setRequesting} />
    </div>
  )
}
