import type { JobStep } from './link-format'
import { stepFill } from './step-ring'

const SIZE = 36
const STROKE = 6
const RADIUS = (SIZE - STROKE) / 2
const CIRCUMFERENCE = 2 * Math.PI * RADIUS
const QUARTER = CIRCUMFERENCE / 4
const GAP = 3
const ARC = QUARTER - GAP

const FILL: Record<JobStep['status'], string> = { done: 'stroke-led', active: 'stroke-primary', waiting: 'stroke-muted' }

function Arc({ index, length, className }: { index: number; length: number; className: string }) {
  if (length <= 0) {
    return null
  }
  return (
    <circle
      cx={SIZE / 2}
      cy={SIZE / 2}
      r={RADIUS}
      fill="none"
      strokeWidth={STROKE}
      className={className}
      strokeDasharray={`${length} ${CIRCUMFERENCE - length}`}
      strokeDashoffset={-(index * QUARTER + GAP / 2)}
    />
  )
}

export function StepRing({ steps }: { steps: JobStep[] }) {
  return (
    <svg width={SIZE} height={SIZE} viewBox={`0 0 ${SIZE} ${SIZE}`} className="-rotate-90" aria-hidden="true">
      {steps.map((step, i) => (
        <g key={step.label}>
          <Arc index={i} length={ARC} className="stroke-muted" />
          <Arc index={i} length={ARC * stepFill(step)} className={FILL[step.status]} />
        </g>
      ))}
    </svg>
  )
}
