import { formatNumber } from '@/lib/format'
import type { HostAnalytics } from '@/types/api'
import { AnalyticsCard, EmptyPanel, PanelState } from './AnalyticsCard'
import { HostBarRow } from './HostBarRow'
import { durationBars, pageBars } from './host-bars'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: HostAnalytics[] }

export function DurationByHostCard({ query }: { query: Query }) {
  const hosts = query.data ?? []
  const { bars, max } = durationBars(hosts)
  return (
    <AnalyticsCard
      kicker={`Bar: median · Tick: p95 · Scale: 0 to ${formatNumber(max)} ms`}
      title="Request duration by host"
    >
      <PanelState query={query}>
        {bars.length === 0 ? <EmptyPanel /> : bars.map((bar) => <HostBarRow key={bar.host} bar={bar} />)}
      </PanelState>
    </AnalyticsCard>
  )
}

export function PagesByHostCard({ query }: { query: Query }) {
  const { bars, total } = pageBars(query.data ?? [])
  return (
    <AnalyticsCard kicker={`Share of ${formatNumber(total)} pages`} title="Pages by host">
      <PanelState query={query}>
        {bars.length === 0 ? <EmptyPanel /> : bars.map((bar) => <HostBarRow key={bar.host} bar={bar} />)}
      </PanelState>
    </AnalyticsCard>
  )
}
