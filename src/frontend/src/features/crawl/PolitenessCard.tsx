import { Card } from '@/components/ui/card'
import { formatMs, formatNumber } from '@/lib/format'
import type { CrawlProgress } from '@/types/api'
import { delaySourceWord, formatSeconds } from './crawl-format'
import { useNow } from './useNow'

function nextRequest(progress: CrawlProgress, now: number): string {
  if (progress.status !== 'Running' || !progress.nextRequestAt) {
    return '—'
  }
  const left = new Date(progress.nextRequestAt).getTime() - now
  return left > 0 ? `in ${formatSeconds(left)}` : 'now'
}

function rows(progress: CrawlProgress, now: number): [string, string][] {
  return [
    ['Delay between requests', formatMs(progress.crawlDelayMs)],
    ['Delay source', delaySourceWord(progress.delaySource)],
    ['Next request', nextRequest(progress, now)],
    ['Median request time', formatMs(progress.avgResponseMs)],
    ['Slowest request', formatMs(progress.maxResponseMs)],
    ['Rate', progress.pagesPerMinute === null ? '—' : `${formatNumber(progress.pagesPerMinute)} pages / min`],
  ]
}

/** How politely SamoBot is crawling: the delay, where it came from and how fast the site answers. */
export function PolitenessCard({ progress }: { progress: CrawlProgress }) {
  const now = useNow(progress.status === 'Running', 100)
  return (
    <Card className="p-5 shadow-zb-1">
      <h2 className="m-0 mb-3 text-base font-semibold">Politeness</h2>
      <dl className="m-0 grid grid-cols-[1fr_auto] gap-x-4 gap-y-2 text-[13px] leading-[18px]">
        {rows(progress, now).map(([term, value]) => (
          <div key={term} className="contents">
            <dt className="text-muted-foreground">{term}</dt>
            <dd className="readout m-0 text-right">{value}</dd>
          </div>
        ))}
      </dl>
    </Card>
  )
}
