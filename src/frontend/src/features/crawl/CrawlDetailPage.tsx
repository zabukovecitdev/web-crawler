import { Link, useParams } from 'react-router-dom'
import { ErrorState, LoadingRows } from '@/components/shared/States'
import type { CrawlJobDto, CrawlProgress } from '@/types/api'
import { ConnectionIndicator } from './ConnectionIndicator'
import { CrawlActions } from './CrawlActions'
import { CurrentRequestCard } from './CurrentRequestCard'
import { FinalSummary } from './FinalSummary'
import { JobStatusBadge } from './JobStatusBadge'
import { PolitenessCard } from './PolitenessCard'
import { RequestLog } from './RequestLog'
import { ResponseBar } from './ResponseBar'
import { StatCards } from './StatCards'
import { formatClock, formatElapsed } from './crawl-format'
import { isFinished } from './crawl-state'
import { useCrawlData } from './useCrawlData'

type HeaderProps = { job: CrawlJobDto; progress: CrawlProgress; live: React.ReactNode }

function CrawlHeader({ job, progress, live }: HeaderProps) {
  const started = progress.startedAt ? ` · started ${formatClock(progress.startedAt)}` : ''
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div className="flex min-w-0 flex-col gap-2">
        <p className="label">
          <Link to="/crawls" className="hover:underline">
            Crawls
          </Link>{' '}
          / Job #{job.id}
          {started}
        </p>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="m-0 break-all text-[32px] font-semibold leading-[38px] tracking-[-0.02em]">{job.host}</h1>
          <JobStatusBadge status={progress.status} />
        </div>
        {live}
      </div>
      {isFinished(progress.status) ? null : <CrawlActions jobId={job.id} host={job.host} status={progress.status} />}
    </div>
  )
}

function elapsedOf(progress: CrawlProgress, endedAt: string | null): string {
  const start = progress.startedAt ? new Date(progress.startedAt).getTime() : null
  const end = endedAt ? new Date(endedAt).getTime() : Date.now()
  return start === null ? '—' : formatElapsed(end - start)
}

function CrawlDetail({ jobId }: { jobId: number }) {
  const { state, job, finished, connection, loading, error, retry } = useCrawlData(jobId)
  const { progress } = state
  if (error) {
    return <ErrorState error={error} onRetry={retry} />
  }
  if (loading || !progress || !job.data) {
    return <LoadingRows rows={8} />
  }
  const endedAt = job.data.completedAt
  const live = finished ? null : <ConnectionIndicator state={connection} />
  return (
    <div className="flex flex-col gap-6">
      <CrawlHeader job={job.data} progress={progress} live={live} />
      {finished ? <FinalSummary progress={progress} host={job.data.host} elapsed={elapsedOf(progress, endedAt)} /> : null}
      <StatCards progress={progress} endedAt={finished ? endedAt : null} />
      <div className="grid grid-cols-1 items-start gap-6 lg:grid-cols-[minmax(0,1fr)_380px]">
        <div className="order-last min-w-0 lg:order-first">
          <RequestLog rows={state.log} />
        </div>
        <div className="flex min-w-0 flex-col gap-6">
          <CurrentRequestCard progress={progress} current={state.current} />
          <PolitenessCard progress={progress} />
          <ResponseBar counts={progress.statusCounts} />
        </div>
      </div>
    </div>
  )
}

export function CrawlDetailPage() {
  const { jobId } = useParams()
  const id = Number(jobId)
  if (!Number.isInteger(id)) {
    return <ErrorState error={new Error('This crawl does not exist.')} />
  }
  return <CrawlDetail key={id} jobId={id} />
}
