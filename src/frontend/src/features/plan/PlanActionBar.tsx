import { Button } from '@/components/ui/button'
import type { Summary } from './plan-summary'

type Props = { summary: Summary; disabled: boolean; pending: boolean; error: string | null; why: string | null; onStart: () => void }

export function PlanActionBar({ summary, disabled, pending, error, why, onStart }: Props) {
  return (
    <div role="region" aria-label="Crawl summary" className="sticky bottom-4 z-10 flex flex-wrap items-center justify-between gap-4 rounded-lg border bg-card p-4 shadow-zb-2">
      <div className="flex min-w-0 flex-col gap-0.5">
        <p className="m-0">
          <span className="readout-lg">{summary.value}</span> <span className="font-semibold">{summary.label}</span>
        </p>
        <p className="m-0 text-[13px] text-muted-foreground">{summary.sub}</p>
        {why ? <p className="m-0 text-[13px] text-signal-ink">{why}</p> : null}
        {error ? (
          <p role="alert" className="m-0 text-[13px] text-signal-ink">
            <strong>The crawl was not started.</strong> {error}
          </p>
        ) : null}
      </div>
      <Button disabled={disabled || pending} onClick={onStart}>
        {pending ? 'Starting…' : 'Start crawl'}
      </Button>
    </div>
  )
}
