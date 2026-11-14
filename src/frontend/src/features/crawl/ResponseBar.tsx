import { Card } from '@/components/ui/card'
import { formatNumber } from '@/lib/format'
import type { CrawlProgress } from '@/types/api'

type Segment = { key: keyof CrawlProgress['statusCounts']; word: string; color: string }

const SEGMENTS: Segment[] = [
  { key: 'ok', word: '2xx OK', color: 'bg-led' },
  { key: 'redirect', word: '3xx Redirect', color: 'bg-ink-faint' },
  { key: 'clientError', word: '4xx Client error', color: 'bg-amber-ink' },
  { key: 'serverError', word: '5xx Server error', color: 'bg-signal-500' },
]

/** Stacked bar of response classes. The legend repeats every number in words. */
export function ResponseBar({ counts }: { counts: CrawlProgress['statusCounts'] }) {
  const summary = SEGMENTS.map((s) => `${formatNumber(counts[s.key])} ${s.word}`).join(', ')
  return (
    <Card className="p-5 shadow-zb-1">
      <h2 className="m-0 mb-3 text-base font-semibold">Responses so far</h2>
      <div role="img" aria-label={`Responses: ${summary}`} className="flex h-3 gap-0.5 overflow-hidden rounded-sm bg-muted">
        {SEGMENTS.map((s) => (
          <i key={s.key} className={`block ${s.color}`} style={{ flex: `${counts[s.key]} 1 0` }} />
        ))}
      </div>
      <dl className="m-0 mt-4 grid grid-cols-[1fr_auto] gap-x-4 gap-y-2 text-[13px] leading-[18px]">
        {SEGMENTS.map((s) => (
          <div key={s.key} className="contents">
            <dt className="flex items-center gap-2 text-muted-foreground">
              <span aria-hidden="true" className={`inline-block size-2.5 rounded-sm ${s.color}`} />
              {s.word}
            </dt>
            <dd className="readout m-0 text-right">{formatNumber(counts[s.key])}</dd>
          </div>
        ))}
      </dl>
    </Card>
  )
}
