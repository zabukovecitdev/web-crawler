import { useState } from 'react'
import { Link } from 'react-router-dom'
import { formatBytes, formatMs, formatNumber, formatPercent } from '@/lib/format'
import type { HostAnalytics } from '@/types/api'
import { AnalyticsCard, EmptyPanel, PanelState } from './AnalyticsCard'
import { DEFAULT_SORT, nextSort, type Sort, type SortKey, sortHosts } from './host-sort'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: HostAnalytics[] }

const COLUMNS: { key: SortKey; label: string }[] = [
  { key: 'host', label: 'Host' },
  { key: 'pages', label: 'Pages' },
  { key: 'requests', label: 'Requests' },
  { key: 'p50ResponseMs', label: 'Median' },
  { key: 'p95ResponseMs', label: 'p95' },
  { key: 'maxResponseMs', label: 'Slowest' },
  { key: 'bytes', label: 'Stored' },
  { key: 'failureRate', label: 'Failed' },
  { key: 'crawlDelayMs', label: 'Delay' },
]

function ariaSort(sort: Sort, key: SortKey) {
  if (sort.key !== key) {
    return 'none'
  }
  return sort.dir === 'asc' ? 'ascending' : 'descending'
}

function Head({ sort, onSort }: { sort: Sort; onSort: (key: SortKey) => void }) {
  return (
    <thead>
      <tr className="border-b">
        {COLUMNS.map((c) => (
          <th key={c.key} scope="col" aria-sort={ariaSort(sort, c.key)} className={c.key === 'host' ? 'py-2 pr-3 text-left' : 'py-2 pl-3 text-right'}>
            <button type="button" className="label inline-flex items-center gap-1 hover:text-foreground" onClick={() => onSort(c.key)}>
              {c.label}
              <span aria-hidden="true">{sort.key === c.key ? (sort.dir === 'asc' ? '↑' : '↓') : ''}</span>
            </button>
          </th>
        ))}
      </tr>
    </thead>
  )
}

function Row({ h }: { h: HostAnalytics }) {
  const cells = [
    formatNumber(h.pages),
    formatNumber(h.requests),
    formatMs(h.p50ResponseMs),
    formatMs(h.p95ResponseMs),
    formatMs(h.maxResponseMs),
    formatBytes(h.bytes),
    formatPercent(h.failureRate),
    formatMs(h.crawlDelayMs),
  ]
  return (
    <tr className="border-b last:border-0">
      <td className="py-2 pr-3">
        <Link className="hover:underline" to={`/sites/${h.host}`}>
          {h.host}
        </Link>
      </td>
      {cells.map((text, i) => (
        <td key={COLUMNS[i + 1].key} className="readout whitespace-nowrap py-2 pl-3 text-right">
          {text}
        </td>
      ))}
    </tr>
  )
}

export function HostsTable({ query }: { query: Query }) {
  const [sort, setSort] = useState<Sort>(DEFAULT_SORT)
  const rows = sortHosts(query.data ?? [], sort)
  return (
    <AnalyticsCard title="All hosts">
      <PanelState query={query}>
        {rows.length === 0 ? (
          <EmptyPanel />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[820px] text-[13px] leading-[18px]">
              <caption className="sr-only">All hosts, sortable by column</caption>
              <Head sort={sort} onSort={(key) => setSort((s) => nextSort(s, key))} />
              <tbody>
                {rows.map((h) => (
                  <Row key={h.host} h={h} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </PanelState>
    </AnalyticsCard>
  )
}
