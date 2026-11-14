import { ErrorState } from '@/components/shared/States'
import { PageHeader } from '@/components/shared/PageHeader'
import { formatDate } from '@/lib/format'
import { useContentTypes, useHostAnalytics, useOverview, useStatusCodes, useTimeseries } from './analytics-api'
import { ContentTypeCard } from './ContentTypeCard'
import { DurationByHostCard, PagesByHostCard } from './HostBarCards'
import { HostFilter } from './HostFilter'
import { HostsTable } from './HostsTable'
import { RangeTabs } from './RangeTabs'
import { StatCards } from './StatCards'
import { StatusCard } from './StatusCard'
import { TimeseriesCard } from './TimeseriesCard'
import { rangePhrase, useFilters } from './range'

export function AnalyticsPage() {
  const { range, host, setRange, setHost } = useFilters()
  const overview = useOverview(range)
  const hosts = useHostAnalytics(range)
  const status = useStatusCodes(range, host)
  const content = useContentTypes(range, host)
  const series = useTimeseries(range, host)
  const o = overview.data
  const period = o?.from && o.to ? `${formatDate(o.from)} to ${formatDate(o.to)}` : rangePhrase(range)
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        kicker={`${host ?? 'All sites'} · ${period}`}
        title="Analytics"
        actions={
          <>
            <HostFilter hosts={hosts.data ?? []} value={host} onChange={setHost} />
            <RangeTabs value={range} onChange={setRange} />
          </>
        }
      />
      {overview.isError ? <ErrorState error={overview.error} onRetry={() => void overview.refetch()} /> : null}
      <StatCards overview={o} failed={overview.isError} />
      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-2">
        <DurationByHostCard query={hosts} />
        <PagesByHostCard query={hosts} />
      </div>
      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-2">
        <StatusCard query={status} />
        <ContentTypeCard query={content} />
        <TimeseriesCard query={series} range={range} />
      </div>
      <HostsTable query={hosts} />
    </div>
  )
}
