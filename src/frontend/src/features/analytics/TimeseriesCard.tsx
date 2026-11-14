import { Bar, BarChart, CartesianGrid, Cell, LabelList, XAxis, YAxis } from 'recharts'
import { type ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart'
import { formatNumber } from '@/lib/format'
import type { AnalyticsRange, TimeseriesPoint } from '@/types/api'
import { AnalyticsCard, EmptyPanel, PanelState } from './AnalyticsCard'
import { type Bucket, bucketFor } from './analytics-api'
import { niceMax } from './host-bars'
import { PeakLabel } from './PeakLabel'
import { rangePhrase } from './range'
import { type Column, peakIndex, seriesKicker, seriesSummary, toColumns } from './timeseries'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: TimeseriesPoint[] }

const CONFIG: ChartConfig = { requests: { label: 'Requests', color: 'rgb(var(--chart-1))' } }
const SIGNAL = 'rgb(var(--primary))'

function Columns({ columns, bucket }: { columns: Column[]; bucket: Bucket }) {
  const peak = peakIndex(columns)
  const last = columns.length - 1
  return (
    <ChartContainer config={CONFIG} className="aspect-auto h-[200px] w-full" role="img" aria-label={seriesSummary(columns, bucket)}>
      <BarChart data={columns} margin={{ top: 20, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid vertical={false} stroke="rgb(var(--border))" strokeOpacity={0.6} />
        <XAxis dataKey="label" tickLine={false} axisLine={{ stroke: 'rgb(var(--line-strong))' }} interval="preserveStartEnd" minTickGap={28} />
        <YAxis width={44} domain={[0, (max: number) => niceMax(max)]} tickCount={5} tickLine={false} axisLine={false} tickFormatter={formatNumber} allowDecimals={false} />
        <ChartTooltip
          cursor={{ fill: 'rgb(var(--muted))' }}
          content={<ChartTooltipContent hideIndicator labelFormatter={(_, p) => String(p[0]?.payload?.tip ?? '')} />}
        />
        <Bar dataKey="requests" fill="var(--color-requests)" radius={[3, 3, 0, 0]} maxBarSize={28} isAnimationActive={false}>
          {columns.map((c, i) => (
            <Cell key={c.tip} fill={i === last ? SIGNAL : 'var(--color-requests)'} />
          ))}
          <LabelList dataKey="requests" content={(p) => <PeakLabel {...p} peak={peak} />} />
        </Bar>
      </BarChart>
    </ChartContainer>
  )
}

export function TimeseriesCard({ query, range }: { query: Query; range: AnalyticsRange }) {
  const bucket = bucketFor(range)
  const columns = toColumns(query.data ?? [], bucket)
  const empty = columns.every((c) => c.requests === 0)
  return (
    <AnalyticsCard
      kicker={seriesKicker(columns, bucket, rangePhrase(range))}
      title={bucket === 'hour' ? 'Requests per hour' : 'Requests per day'}
      className="lg:col-span-2"
    >
      <PanelState query={query}>{empty ? <EmptyPanel /> : <Columns columns={columns} bucket={bucket} />}</PanelState>
    </AnalyticsCard>
  )
}
