import { describe, expect, it } from 'vitest'
import { formatRange } from './format-range'

describe('formatRange', () => {
  it.each([
    ['2026-11-18T00:00:00Z', '2026-11-18T10:00:00Z', '18 Nov 2026'],
    ['2026-11-02T00:00:00Z', '2026-11-18T00:00:00Z', 'Nov 2026'],
    ['2026-01-05T00:00:00Z', '2026-11-18T00:00:00Z', 'Jan – Nov 2026'],
    ['2016-01-05T00:00:00Z', '2023-11-18T00:00:00Z', '2016 – 2023'],
    [null, '2023-11-18T00:00:00Z', ''],
    ['2023-11-18T00:00:00Z', null, ''],
  ])('%s to %s reads %s', (from, to, text) => {
    expect(formatRange(from, to)).toBe(text)
  })
})
