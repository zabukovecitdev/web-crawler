import { formatNumber } from '@/lib/format'
import type { CrawlJobDto } from '@/types/api'
import { jobHeadline, jobSteps, problemsText, type JobStep } from './link-format'

const BAR: Record<JobStep['status'], string> = { done: 'bg-led', active: 'bg-primary', waiting: 'bg-ink-faint' }
const MARK: Record<JobStep['status'], string> = { done: '✓', active: '…', waiting: '·' }

function Step({ step, index }: { step: JobStep; index: number }) {
  const percent = step.of > 0 ? Math.min(100, Math.round((step.done / step.of) * 100)) : 0
  return (
    <li className="flex min-w-[92px] flex-1 flex-col gap-1" aria-label={`${step.label}: ${step.done} of ${step.of} ${step.note}, ${step.status}`}>
      <span className="label flex items-center gap-1.5">
        <span aria-hidden="true">{MARK[step.status]}</span>
        {index + 1} {step.label}
      </span>
      <div className="h-1.5 overflow-hidden rounded-full bg-muted">
        <div className={`h-full rounded-full ${BAR[step.status]}`} style={{ width: `${percent}%` }} />
      </div>
      <span className="readout text-[13px]">
        {formatNumber(step.done)} / {formatNumber(Math.max(step.of, 0))}
      </span>
    </li>
  )
}

function Notes({ job }: { job: CrawlJobDto }) {
  const { links } = job
  const problems = links.failed + links.retrying
  return (
    <div className="flex flex-col gap-1 text-[13px] leading-[18px]">
      {problems > 0 ? (
        <p className="m-0 text-signal-ink">
          <strong>{formatNumber(problems)} with problems:</strong> {problemsText(links)}
        </p>
      ) : null}
      {links.skipped + links.notIndexed > 0 ? (
        <p className="m-0 text-muted-foreground">
          {formatNumber(links.skipped)} skipped (robots.txt, redirects, files that are not HTML), {formatNumber(links.notIndexed)} kept out
          of search by the site.
        </p>
      ) : null}
    </div>
  )
}

/** What the job has done so far: one sentence, the four steps with counts, and why links failed. */
export function JobSteps({ job }: { job: CrawlJobDto }) {
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <p className="m-0 text-sm font-medium">{jobHeadline(job)}</p>
      <ol className="m-0 flex list-none flex-wrap gap-3 p-0">
        {jobSteps(job.links).map((step, index) => (
          <Step key={step.label} step={step} index={index} />
        ))}
      </ol>
      <Notes job={job} />
    </div>
  )
}
