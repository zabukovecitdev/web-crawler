import { expect, it } from 'vitest'
import { noticeKinds } from './plan-notices'

const base = { mode: 'Auto', verdict: 'NotNeeded', browserUp: true, sitemapWaitedTooLong: false } as const

it('shows no notice for a healthy static site', () => {
  expect(noticeKinds(base)).toEqual({ gate: false, never: false, browserInfo: false, sitemapSlow: false })
})

it('blocks the start when a site that needs JavaScript has no browser', () => {
  expect(noticeKinds({ ...base, verdict: 'Needed', browserUp: false })).toMatchObject({ gate: true, browserInfo: false })
})

it('only informs when a site that does not need the browser meets none', () => {
  expect(noticeKinds({ ...base, browserUp: false })).toMatchObject({ gate: false, browserInfo: true })
})

it('warns about empty pages when JavaScript is turned off for a site that needs it', () => {
  expect(noticeKinds({ ...base, mode: 'Never', verdict: 'ProbablyNeeded', browserUp: false })).toMatchObject({ gate: false, never: true, browserInfo: false })
})

it('blocks always without a browser and mentions a slow sitemap', () => {
  expect(noticeKinds({ ...base, mode: 'Always', browserUp: false, sitemapWaitedTooLong: true })).toMatchObject({ gate: true, sitemapSlow: true })
})
