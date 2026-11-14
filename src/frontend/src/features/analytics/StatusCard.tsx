import { Bar, BarChart, XAxis, YAxis } from 'recharts'
import { type ChartConfig, ChartContainer } from '@/components/ui/chart'
import { formatNumber } from '@/lib/format'
import type { StatusClassCounts } from '@/types/api'
import { AnalyticsCard, EmptyPanel, PanelState } from './AnalyticsCard'
import { type StatusRow, statusRows, statusSummary, statusTotal } from './status'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: StatusClassCounts }

const CONFIG: ChartConfig = {}

function StackedBar({ rows }: { rows: StatusRow[] }) {
  const data = [Object.fromEntries(rows.map((r) => [r.key, r.count]))]
  return (
    <ChartContainer
      config={CONFIG}
      className="aspect-auto h-6 w-full"
      role="img"
      aria-label={`Response status: ${statusSummary(rows)}`}
    >
      <BarChart layout="vertical" data={data} stackOffset="expand" margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
        <XAxis type="number" hide domain={[0, 1]} />
        <YAxis type="category" hide />
        {rows.map((r) => (
          <Bar key={r.key} dataKey={r.key} stackId="s" fill={r.color} barSize={20} stroke="rgb(var(--card))" strokeWidth={1} isAnimationActive={false} />
        ))}
      </BarChart>
    </ChartContainer>
  )
}

function Legend({ rows }: { rows: StatusRow[] }) {
  return (
    <table className="w-full text-[13px] leading-[18px]">
      <caption className="sr-only">Response status counts</caption>
      <tbody>
        {rows.map((r) => (
          <tr key={r.key}>
            <th scope="row" className="py-1 text-left font-normal">
              <span aria-hidden="true" className="mr-2 inline-block size-2.5 rounded-[3px] align-[-1px]" style={{ background: r.color }} />
              {r.label}
            </th>
            <td className="readout py-1 text-right">
              {formatNumber(r.count)} · {r.share}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export function StatusCard({ query }: { query: Query }) {
  const counts = query.data
  const total = counts ? statusTotal(counts) : 0
  return (
    <AnalyticsCard kicker={`${formatNumber(total)} requests`} title="Response status">
      <PanelState query={query}>
        {counts && total > 0 ? (
          <>
            <StackedBar rows={statusRows(counts)} />
            <Legend rows={statusRows(counts)} />
          </>
        ) : (
          <EmptyPanel />
        )}
      </PanelState>
    </AnalyticsCard>
  )
}
