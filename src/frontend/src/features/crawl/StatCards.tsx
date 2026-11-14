import { StatCard } from '@/components/shared/StatCard'
import { Card } from '@/components/ui/card'
import { formatNumber } from '@/lib/format'
import type { CrawlProgress } from '@/types/api'
import { formatElapsed, percentOf, remainingNote } from './crawl-format'
import { useNow } from './useNow'

const LIVE = { 'aria-live': 'polite', 'aria-atomic': true } as const

function PagesCard({ progress }: { progress: CrawlProgress }) {
  const percent = percentOf(progress.crawled, progress.maxUrls)
  return (
    <Card className="flex flex-col gap-1 p-5 shadow-zb-1">
      <p className="label">Pages crawled</p>
      <div className="readout-lg" {...LIVE}>
        {formatNumber(progress.crawled)}
      </div>
      {percent === null ? (
        <p className="text-[13px] leading-[18px] text-muted-foreground">No page limit</p>
      ) : (
        <>
          <div
            role="img"
            aria-label={`${formatNumber(progress.crawled)} of ${formatNumber(progress.maxUrls)} pages, ${percent} percent`}
            className="my-2 h-2 overflow-hidden rounded-full bg-muted"
          >
            <div className="h-full rounded-full bg-primary" style={{ width: `${percent}%` }} />
          </div>
          <p className="text-[13px] leading-[18px] text-muted-foreground">
            {percent} % of the {formatNumber(progress.maxUrls)} page limit
          </p>
        </>
      )}
    </Card>
  )
}

type ElapsedProps = { progress: CrawlProgress; endedAt: string | null }

function ElapsedCard({ progress, endedAt }: ElapsedProps) {
  const running = progress.status === 'Running'
  const now = useNow(running, 1000)
  const start = progress.startedAt ? new Date(progress.startedAt).getTime() : null
  const end = endedAt ? new Date(endedAt).getTime() : now
  return (
    <StatCard
      kicker="Elapsed"
      value={start === null ? '—' : formatElapsed(end - start)}
      note={remainingNote(progress) ?? (running ? 'Rate is not known yet' : 'Crawl is not running')}
    />
  )
}

/** Pages, queue, failed, elapsed. The counters announce changes politely. */
export function StatCards({ progress, endedAt }: ElapsedProps) {
  return (
    <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 xl:grid-cols-4">
      <PagesCard progress={progress} />
      <StatCard kicker="In queue" value={<span {...LIVE}>{formatNumber(progress.queued)}</span>} note="Found, not fetched yet" />
      <StatCard kicker="Failed" value={<span {...LIVE}>{formatNumber(progress.failed)}</span>} note="4xx, 5xx and timeouts" />
      <ElapsedCard progress={progress} endedAt={endedAt} />
    </div>
  )
}
