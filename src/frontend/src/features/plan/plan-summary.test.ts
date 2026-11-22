import { describe, expect, it } from 'vitest'
import { formatEstimate, formatNumber, secondsPerPage, summarize, usesBrowser } from './plan-summary'

describe('formatEstimate', () => {
  it.each([
    [0, '0 min'],
    [20, 'under 1 min'],
    [31 * 60, '31 min'],
    [60 * 60, '1 h'],
    [(2 * 60 + 5) * 60, '2 h 5 min'],
    [(2 * 60 + 59.9) * 60, '3 h'],
  ])('%s s reads %s', (seconds, text) => {
    expect(formatEstimate(seconds)).toBe(text)
  })
})

describe('usesBrowser', () => {
  it.each([
    ['Always', 'NotNeeded', true],
    ['Auto', 'Needed', true],
    ['Auto', 'ProbablyNeeded', true],
    ['Auto', 'NotNeeded', false],
    ['Never', 'Needed', false],
  ] as const)('mode %s with %s: %s', (mode, verdict, expected) => {
    expect(usesBrowser(mode, verdict)).toBe(expected)
  })
})

describe('secondsPerPage', () => {
  it('is at least one second over HTTP and three in a browser', () => {
    expect([secondsPerPage(null, false), secondsPerPage(0.5, false), secondsPerPage(2, false), secondsPerPage(1, true), secondsPerPage(5, true)]).toEqual([1, 1, 2, 3, 5])
  })
})

describe('summarize', () => {
  it('reads like the plan of a site with a sitemap', () => {
    const summary = summarize({ ticked: 1843, total: 1893, hasSitemap: true, seconds: 1843 * 1, inBrowser: false, linkedExtra: true, review: true })

    expect(summary).toEqual({
      value: '1,843',
      label: 'pages ticked',
      sub: 'of 1,893 in the sitemap · about 31 min · HTTP · plus linked pages that are not in the sitemap',
    })
  })

  it('uses the singular for one page and names the browser', () => {
    const summary = summarize({ ticked: 1, total: 1, hasSitemap: true, seconds: 3, inBrowser: true, linkedExtra: false, review: false })

    expect([summary.label, summary.sub]).toEqual(['page ticked', 'of 1 in the sitemap · about under 1 min · browser'])
  })

  it('tells that more pages are found while crawling when there is no sitemap', () => {
    const summary = summarize({ ticked: 12, total: 12, hasSitemap: false, seconds: 0, inBrowser: false, linkedExtra: false, review: true })

    expect(summary.label).toBe('pages known so far')
    expect(summary.sub).toContain('pauses after 100 pages')
    expect(formatNumber(1234567)).toBe('1,234,567')
  })
})
