import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatNumber, formatRelative } from '@/lib/format'
import type { CrawlJobDto } from '@/types/api'
import { JobStatusBadge } from './JobStatusBadge'
import { crawlKeys, fetchProgress } from './crawl-api'
import { percentOf } from './crawl-format'
import { isFinished } from './crawl-state'

function ProgressCell({ job }: { job: CrawlJobDto }) {
  const running = job.status === 'Running'
  const progress = useQuery({
    queryKey: crawlKeys.progress(job.id),
    queryFn: () => fetchProgress(job.id),
    refetchInterval: running ? 5000 : false,
    staleTime: isFinished(job.status) ? Infinity : 0,
  })
  if (!progress.data) {
    return <span className="readout text-muted-foreground">—</span>
  }
  const percent = percentOf(progress.data.crawled, job.maxUrls)
  return (
    <div className="flex min-w-[140px] flex-col gap-1.5">
      <span className="readout">
        {formatNumber(progress.data.crawled)}
        {job.maxUrls ? ` / ${formatNumber(job.maxUrls)}` : ''} pages
      </span>
      {percent === null ? null : (
        <div role="img" aria-label={`${percent} percent of the page limit`} className="h-1.5 overflow-hidden rounded-full bg-muted">
          <div className={`h-full rounded-full ${running ? 'bg-primary' : 'bg-ink-faint'}`} style={{ width: `${percent}%` }} />
        </div>
      )}
    </div>
  )
}

function JobRow({ job }: { job: CrawlJobDto }) {
  return (
    <TableRow>
      <TableCell>
        <JobStatusBadge status={job.status} />
      </TableCell>
      <TableCell className="readout break-all">
        <Link to={`/crawls/${job.id}`} className="font-medium underline-offset-4 hover:underline">
          {job.host}
        </Link>
        <span className="ml-2 text-muted-foreground">#{job.id}</span>
      </TableCell>
      <TableCell>
        <ProgressCell job={job} />
      </TableCell>
      <TableCell className="whitespace-nowrap">{formatRelative(job.startedAt ?? job.createdAt)}</TableCell>
    </TableRow>
  )
}

export function CrawlsTable({ jobs }: { jobs: CrawlJobDto[] }) {
  return (
    <Table className="min-w-[560px]">
      <TableHeader>
        <TableRow>
          <TableHead className="label">Status</TableHead>
          <TableHead className="label">Site</TableHead>
          <TableHead className="label">Progress</TableHead>
          <TableHead className="label">Started</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {jobs.map((job) => (
          <JobRow key={job.id} job={job} />
        ))}
      </TableBody>
    </Table>
  )
}
