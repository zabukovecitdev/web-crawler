import { describe, expect, it } from 'vitest'
import type { SiteCheckDto } from '@/types/api'
import { buildJobRequest } from './job-request'

const check = {
  startUrl: 'https://docs.example.com/guide/install',
  javaScript: { verdict: 'NotNeeded' },
} as SiteCheckDto

const rules = [{ kind: 'Exclude' as const, prefix: '/blog/2016/' }]

describe('buildJobRequest', () => {
  it('seeds from the sitemap and follows linked pages when asked', () => {
    expect(buildJobRequest(check, { rules, mode: 'Auto', linked: true, review: true, hasSitemap: true })).toEqual({
      seedUrls: ['https://docs.example.com/guide/install'],
      renderMode: 'Auto',
      jsNeeded: false,
      pathRules: rules,
      seedFromSitemap: true,
      followLinkedPages: true,
      reviewAfterPages: undefined,
      startImmediately: true,
    })
  })

  it('can leave out the pages that are linked but not in the sitemap', () => {
    expect(buildJobRequest(check, { rules: [], mode: 'Never', linked: false, review: true, hasSitemap: true }).followLinkedPages).toBe(false)
  })

  it('always follows links without a sitemap and asks for the review after 100 pages', () => {
    const request = buildJobRequest(check, { rules: [], mode: 'Auto', linked: false, review: true, hasSitemap: false })

    expect([request.seedFromSitemap, request.followLinkedPages, request.reviewAfterPages]).toEqual([false, true, 100])
  })

  it('does not review when it was turned off', () => {
    expect(buildJobRequest(check, { rules: [], mode: 'Auto', linked: true, review: false, hasSitemap: false }).reviewAfterPages).toBeUndefined()
  })

  it('passes on that the site needs JavaScript, also when it is only probably so', () => {
    const needy = { ...check, javaScript: { verdict: 'ProbablyNeeded' } } as SiteCheckDto

    expect(buildJobRequest(needy, { rules: [], mode: 'Auto', linked: true, review: false, hasSitemap: true }).jsNeeded).toBe(true)
  })
})
