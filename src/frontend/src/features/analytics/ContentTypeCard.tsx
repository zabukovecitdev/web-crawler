import { formatNumber } from '@/lib/format'
import type { ContentTypeCount } from '@/types/api'
import { AnalyticsCard, EmptyPanel, PanelState } from './AnalyticsCard'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: ContentTypeCount[] }

function ContentTable({ rows }: { rows: ContentTypeCount[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[13px] leading-[18px]">
        <caption className="sr-only">Requests by content type</caption>
        <thead>
          <tr className="border-b text-left">
            <th scope="col" className="label py-2 pr-3">Type</th>
            <th scope="col" className="label py-2 pr-3 text-right">Requests</th>
            <th scope="col" className="label py-2">Content</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.contentType} className="border-b last:border-0">
              <td className="readout py-2 pr-3">{r.contentType}</td>
              <td className="readout py-2 pr-3 text-right">{formatNumber(r.requests)}</td>
              <td className="py-2">{r.stored ? 'Stored' : 'Skipped'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function ContentTypeCard({ query }: { query: Query }) {
  const rows = [...(query.data ?? [])].sort((a, b) => b.requests - a.requests)
  return (
    <AnalyticsCard kicker="By requests" title="Content type">
      <PanelState query={query}>{rows.length ? <ContentTable rows={rows} /> : <EmptyPanel />}</PanelState>
    </AnalyticsCard>
  )
}
