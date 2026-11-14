import { describe, expect, it } from 'vitest'
import { formatBytes, formatDate, formatMs, formatNumber, formatPercent, formatRelative } from './format'

const DASH = '—'

describe('formatNumber', () => {
  it('groups thousands with a thin space', () => {
    expect(formatNumber(12482)).toBe('12 482')
  })

  it('prints a dash for missing and non-finite input', () => {
    expect(formatNumber(null)).toBe(DASH)
    expect(formatNumber(undefined)).toBe(DASH)
    expect(formatNumber(Number.NaN)).toBe(DASH)
    expect(formatNumber(Number.POSITIVE_INFINITY)).toBe(DASH)
  })
})

describe('formatMs, formatBytes, formatPercent', () => {
  it('format finite values', () => {
    expect(formatMs(250)).toBe('250 ms')
    expect(formatBytes(2048)).toBe('2.0 KB')
    expect(formatPercent(0.5)).toBe('50 %')
  })

  it('print a dash for non-finite values', () => {
    expect(formatMs(Number.NaN)).toBe(DASH)
    expect(formatBytes(Number.POSITIVE_INFINITY)).toBe(DASH)
    expect(formatPercent(Number.NaN)).toBe(DASH)
  })
})

describe('formatDate and formatRelative', () => {
  it('print a dash or Never for an unparsable date', () => {
    expect(formatDate('not a date')).toBe(DASH)
    expect(formatRelative('not a date')).toBe(DASH)
    expect(formatRelative(null)).toBe('Never')
  })

  it('formats relative minutes', () => {
    const now = Date.parse('2024-01-01T12:10:00Z')
    expect(formatRelative('2024-01-01T12:05:00Z', now)).toBe('5 minutes ago')
  })
})
