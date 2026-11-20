import { formatNumber } from '@/lib/format'
import type { JobStep } from './link-format'

const WORD: Record<JobStep['status'], string> = { done: 'done', active: 'in progress', waiting: 'not started' }

export function stepFill(step: JobStep): number {
  if (step.status === 'done') {
    return 1
  }
  if (step.status === 'waiting' || step.of <= 0) {
    return 0
  }
  return Math.min(1, Math.max(0.08, step.done / step.of))
}

export function ringLabel(steps: JobStep[]): string {
  return steps.map((s) => `${s.label}: ${formatNumber(s.done)} of ${formatNumber(Math.max(s.of, 0))}, ${WORD[s.status]}`).join('. ')
}
