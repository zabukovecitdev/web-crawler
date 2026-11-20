import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { formatNumber, formatRelative } from '@/lib/format'
import type { CrawlJobDto } from '@/types/api'
import { groupOf } from './crawl-groups'
import { JobLinksPanel } from './JobLinksPanel'
import { JobStatusBadge } from './JobStatusBadge'
import { JobSteps } from './JobSteps'
import { jobHeadline, jobSteps } from './link-format'
import { RecrawlActions } from './RecrawlActions'
import { StepRing } from './StepRing'
import { ringLabel } from './step-ring'

function whenText(job: CrawlJobDto): string {
  if (job.completedAt) {
    return `finished ${formatRelative(job.completedAt)}`
  }
  return `started ${formatRelative(job.startedAt ?? job.createdAt)}`
}

function Summary({ job }: { job: CrawlJobDto }) {
  const problems = groupOf(job.status) === 'active' ? job.links.failed + job.links.retrying : 0
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <Link to={`/crawls/${job.id}`} className="readout break-all text-base font-semibold underline-offset-4 hover:underline">
          {job.host}
        </Link>
        <JobStatusBadge status={job.status} />
        <span className="readout text-[13px] text-muted-foreground">
          #{job.id} · {whenText(job)}
        </span>
      </div>
      <p className="m-0 text-[13px] leading-[18px] text-muted-foreground">
        {jobHeadline(job)}
        {problems > 0 ? <span className="text-signal-ink"> {formatNumber(problems)} with problems.</span> : null}
      </p>
    </div>
  )
}

function Details({ job }: { job: CrawlJobDto }) {
  const [linksOpen, setLinksOpen] = useState(false)
  const panelId = `job-${job.id}-links`
  return (
    <div className="flex flex-col gap-4 pl-[52px]">
      <JobSteps job={job} headline={false} />
      <div>
        <Button variant="ghost" size="sm" aria-expanded={linksOpen} aria-controls={panelId} onClick={() => setLinksOpen(!linksOpen)}>
          {linksOpen ? 'Hide links' : `Show links (${formatNumber(job.links.total)})`}
        </Button>
      </div>
      {linksOpen ? (
        <div id={panelId}>
          <JobLinksPanel job={job} />
        </div>
      ) : null}
    </div>
  )
}

function JobItem({ job }: { job: CrawlJobDto }) {
  const [open, setOpen] = useState(false)
  const steps = jobSteps(job.links)
  const detailsId = `job-${job.id}-details`
  const label = ringLabel(steps)
  return (
    <li className="flex flex-col gap-4 border-t py-4 first:border-t-0 first:pt-0 last:pb-0">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex min-w-0 items-center gap-4">
          <button
            type="button"
            title={`${label}. ${open ? 'Hide' : 'Show'} details.`}
            aria-label={`${open ? 'Hide' : 'Show'} progress details of ${job.host}. ${label}`}
            aria-expanded={open}
            aria-controls={detailsId}
            onClick={() => setOpen(!open)}
            className="shrink-0 rounded-full p-0.5 hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <StepRing steps={steps} />
          </button>
          <Summary job={job} />
        </div>
        <RecrawlActions job={job} />
      </div>
      {open ? (
        <div id={detailsId}>
          <Details job={job} />
        </div>
      ) : null}
    </li>
  )
}

export function CrawlsTable({ jobs }: { jobs: CrawlJobDto[] }) {
  return (
    <ul className="m-0 flex list-none flex-col p-0">
      {jobs.map((job) => (
        <JobItem key={job.id} job={job} />
      ))}
    </ul>
  )
}
