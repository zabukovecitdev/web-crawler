import { StatCard } from '@/components/shared/StatCard'
import { Led } from '@/components/shared/Led'
import { formatMs, formatNumber } from '@/lib/format'
import type { SiteSummary, SitesOverview } from '@/types/api'

type Props = { overview: SitesOverview; items: SiteSummary[] }

export function SitesStats({ overview, items }: Props) {
  const live = items.find((site) => site.status === 'Crawling')
  return (
    <section aria-label="Overview" className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <StatCard kicker="Sites" value={formatNumber(overview.sites)} note={`${formatNumber(overview.crawling)} crawling now`} />
      <StatCard kicker="Pages" value={formatNumber(overview.pages)} note={`${formatNumber(overview.failed)} failed`} />
      <StatCard
        kicker="Crawling now"
        value={formatNumber(overview.crawling)}
        note={
          live ? (
            <span className="inline-flex items-center gap-1.5">
              <Led /> {live.host}
            </span>
          ) : (
            'No crawl is running'
          )
        }
      />
      <StatCard
        kicker="Median response"
        value={formatMs(overview.medianResponseMs)}
        note={`p95 ${formatMs(overview.p95ResponseMs)}`}
      />
    </section>
  )
}
