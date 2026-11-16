import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { formatRelative } from '@/lib/format'
import type { CrawlJobDto } from '@/types/api'
import { JobLinksPanel } from './JobLinksPanel'
import { JobStatusBadge } from './JobStatusBadge'
import { JobSteps } from './JobSteps'
import { RecrawlActions } from './RecrawlActions'

function JobHeading({ job }: { job: CrawlJobDto }) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      <JobStatusBadge status={job.status} />
      <Link to={`/crawls/${job.id}`} className="readout break-all text-base font-semibold underline-offset-4 hover:underline">
        {job.host}
      </Link>
      <span className="readout text-muted-foreground">
        #{job.id} · started {formatRelative(job.startedAt ?? job.createdAt)}
      </span>
    </div>
  )
}

function JobItem({ job }: { job: CrawlJobDto }) {
  const [open, setOpen] = useState(false)
  const panelId = `job-${job.id}-links`
  return (
    <li className="flex flex-col gap-4 border-t py-5 first:border-t-0 first:pt-0 last:pb-0">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <JobHeading job={job} />
        <RecrawlActions job={job} />
      </div>
      <JobSteps job={job} />
      <div>
        <Button variant="ghost" size="sm" aria-expanded={open} aria-controls={panelId} onClick={() => setOpen(!open)}>
          {open ? 'Hide links' : `Show links (${job.links.total})`}
        </Button>
      </div>
      {open ? (
        <div id={panelId}>
          <JobLinksPanel job={job} />
        </div>
      ) : null}
    </li>
  )
}

/** Every crawl with its status in words, its steps and its problems; the links open in place. */
export function CrawlsTable({ jobs }: { jobs: CrawlJobDto[] }) {
  return (
    <ul className="m-0 flex list-none flex-col p-0">
      {jobs.map((job) => (
        <JobItem key={job.id} job={job} />
      ))}
    </ul>
  )
}
