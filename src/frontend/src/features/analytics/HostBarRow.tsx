import { Link } from 'react-router-dom'
import { Bar, BarChart, ReferenceLine, XAxis, YAxis } from 'recharts'
import { type ChartConfig, ChartContainer } from '@/components/ui/chart'

const NEUTRAL = 'rgb(var(--chart-1))'
const SIGNAL = 'rgb(var(--primary))'
const TRACK = 'rgb(var(--muted))'
const INK = 'rgb(var(--foreground))'

export type HostBar = {
  host: string
  value: number
  max: number
  /** Optional marker (p95) on the same axis. */
  marker?: number | null
  /** The one thing that matters gets the signal colour. */
  highlight?: boolean
  text: string
  summary: string
}

const CONFIG: ChartConfig = { value: { label: 'Value' } }

/** One row: host link, a single-axis bar (with an optional marker), then the value as text. */
export function HostBarRow({ bar }: { bar: HostBar }) {
  return (
    <div className="grid grid-cols-1 items-center gap-1 py-[7px] text-[13px] leading-[18px] sm:grid-cols-[minmax(96px,168px)_minmax(0,1fr)_132px] sm:gap-3">
      <Link className="truncate text-foreground underline-offset-2 hover:underline" to={`/sites/${bar.host}`}>
        {bar.host}
      </Link>
      <ChartContainer
        config={CONFIG}
        className="aspect-auto h-5 w-full"
        role="img"
        aria-label={bar.summary}
      >
        <BarChart layout="vertical" data={[bar]} margin={{ top: 0, right: 2, bottom: 0, left: 0 }}>
          <XAxis type="number" domain={[0, bar.max]} hide />
          <YAxis type="category" dataKey="host" hide />
          <Bar
            dataKey="value"
            barSize={10}
            radius={4}
            fill={bar.highlight ? SIGNAL : NEUTRAL}
            background={{ fill: TRACK, radius: 4 }}
            isAnimationActive={false}
          />
          {bar.marker ? <ReferenceLine x={bar.marker} stroke={INK} strokeWidth={2} /> : null}
        </BarChart>
      </ChartContainer>
      <span className="readout sm:text-right">{bar.text}</span>
    </div>
  )
}
