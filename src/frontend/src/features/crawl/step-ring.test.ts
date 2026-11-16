import { describe, expect, it } from 'vitest'
import { formatNumber } from '@/lib/format'
import type { JobStep } from './link-format'
import { ringLabel, stepFill } from './step-ring'

const step = (status: JobStep['status'], done: number, of: number): JobStep => ({ label: 'Fetch', done, of, status, note: '' })

describe('stepFill', () => {
  it('fills a done step and leaves a waiting one empty', () => {
    expect(stepFill(step('done', 10, 10))).toBe(1)
    expect(stepFill(step('waiting', 0, 10))).toBe(0)
  })

  it('fills a running step by its share, with a sliver so that it shows', () => {
    expect(stepFill(step('active', 5, 10))).toBe(0.5)
    expect(stepFill(step('active', 0, 10))).toBe(0.08)
    expect(stepFill(step('active', 0, 0))).toBe(0)
  })
})

describe('ringLabel', () => {
  it('says each step in words', () => {
    expect(ringLabel([step('active', 1150, 4800), { ...step('waiting', 0, 0), label: 'Store' }])).toBe(
      `Fetch: ${formatNumber(1150)} of ${formatNumber(4800)}, in progress. Store: 0 of 0, not started`,
    )
  })
})
