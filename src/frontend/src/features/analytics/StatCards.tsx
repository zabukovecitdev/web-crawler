import { StatCard } from '@/components/shared/StatCard'
import { Skeleton } from '@/components/ui/skeleton'
import { formatMs, formatNumber, formatPercent } from '@/lib/format'
import type { AnalyticsOverview } from '@/types/api'

type Props = { overview: AnalyticsOverview | undefined; failed: boolean }

function perPage(o: AnalyticsOverview): string {
  return o.pages ? `${(o.requests / o.pages).toFixed(1)} requests for each page` : 'No pages yet'
}

export function StatCards({ overview, failed }: Props) {
  if (!overview) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4" role={failed ? undefined : 'status'} aria-label="Loading">
        {[0, 1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-[108px] w-full rounded-xl" />
        ))}
      </div>
    )
  }
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <StatCard kicker="Pages" value={formatNumber(overview.pages)} note={`On ${formatNumber(overview.hosts)} sites`} />
      <StatCard kicker="Requests" value={formatNumber(overview.requests)} note={perPage(overview)} />
      <StatCard
        kicker="Median request time"
        value={formatMs(overview.p50ResponseMs)}
        note={`p95 ${formatMs(overview.p95ResponseMs)}, slowest ${formatMs(overview.maxResponseMs)}`}
      />
      <StatCard
        kicker="Failed pages"
        value={formatPercent(overview.failureRate)}
        note={`${formatNumber(overview.failedPages)} of ${formatNumber(overview.pages)}`}
      />
    </div>
  )
}
