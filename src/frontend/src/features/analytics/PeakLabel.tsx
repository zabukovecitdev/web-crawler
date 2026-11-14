import { formatNumber } from '@/lib/format'

type Props = {
  x?: number | string
  y?: number | string
  width?: number | string
  value?: number | string
  index?: number
  peak: number
}

/** Direct label above the single peak column; every other column stays unlabelled. */
export function PeakLabel({ x, y, width, value, index, peak }: Props) {
  if (index !== peak) {
    return null
  }
  const cx = Number(x) + Number(width) / 2
  return (
    <text x={cx} y={Number(y) - 6} textAnchor="middle" className="fill-foreground font-mono text-[11px] font-medium">
      {formatNumber(Number(value))}
    </text>
  )
}
