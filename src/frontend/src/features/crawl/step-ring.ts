import { formatNumber } from '@/lib/format'
import type { JobStep } from './link-format'

const WORD: Record<JobStep['status'], string> = { done: 'done', active: 'in progress', waiting: 'not started' }

/** How much of a quarter to fill: all of it when the step is done, the share done while it runs, none before it starts. */
export function stepFill(step: JobStep): number {
  if (step.status === 'done') {
    return 1
  }
  if (step.status === 'waiting' || step.of <= 0) {
    return 0
  }
  return Math.min(1, Math.max(0.08, step.done / step.of))
}

/** "Fetch: 1 150 of 4 800, in progress. Store: …" for screen readers and the tooltip. */
export function ringLabel(steps: JobStep[]): string {
  return steps.map((s) => `${s.label}: ${formatNumber(s.done)} of ${formatNumber(Math.max(s.of, 0))}, ${WORD[s.status]}`).join('. ')
}
