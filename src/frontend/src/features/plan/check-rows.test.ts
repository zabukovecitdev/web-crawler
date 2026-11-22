import { expect, it } from 'vitest'
import type { SiteCheckDto } from '@/types/api'
import { checkRows } from './check-rows'

function check(patch: Partial<SiteCheckDto> = {}): SiteCheckDto {
  return {
    requestedUrl: 'https://example.com/guide/install',
    startUrl: 'https://docs.example.com/guide/install',
    host: 'docs.example.com',
    redirected: true,
    startPage: { statusCode: 200, reachable: true, blockedByRobots: false, problem: null },
    robots: { startAllowed: true, crawlDelaySeconds: 1, disallowed: ['/admin/'] },
    sitemap: { state: 'Ready', files: 3, urls: 1893, checkedAt: null },
    browser: { available: true, reason: null, checkedAt: '2026-11-22T10:00:00Z' },
    javaScript: { verdict: 'NotNeeded', plain: { textLength: 6214, linkCount: 143, emptyAppContainer: false }, rendered: null },
    checkedAt: '2026-11-22T10:00:00Z',
    ...patch,
  }
}

const textOf = (rows: ReturnType<typeof checkRows>, name: string) => rows.find((row) => row.name === name)

it('checkRows reads like the prototype for a healthy static site', () => {
  const rows = checkRows(check())

  expect(rows.map((row) => [row.name, row.tone])).toEqual([
    ['Start page', 'ok'],
    ['robots.txt', 'ok'],
    ['Sitemap', 'ok'],
    ['Browser service', 'ok'],
    ['JavaScript', 'ok'],
  ])
  expect(textOf(rows, 'Start page')?.text).toBe('example.com redirects to docs.example.com, which answers 200 OK. The crawl stays on docs.example.com.')
  expect(textOf(rows, 'robots.txt')?.text).toBe('SamoBot may crawl this site. 1 s between requests. /admin/ is disallowed.')
  expect(textOf(rows, 'Sitemap')?.text).toBe('3 files with 1,893 URLs.')
  expect(textOf(rows, 'JavaScript')?.text).toBe('Not needed. The start page has 6,214 characters and 143 links without JavaScript.')
})

it('names the detected need for JavaScript with both sizes', () => {
  const rows = checkRows(
    check({
      javaScript: {
        verdict: 'Needed',
        plain: { textLength: 84, linkCount: 1, emptyAppContainer: true },
        rendered: { textLength: 5902, linkCount: 131, emptyAppContainer: false },
      },
    }),
  )

  expect(textOf(rows, 'JavaScript')).toEqual({
    name: 'JavaScript',
    tone: 'info',
    text: 'Needed. Without JavaScript the start page has 84 characters and 1 link. In a browser it has 5,902 characters and 131 links.',
  })
})

it('says probably needed, not confirmed, when the browser is down', () => {
  const rows = checkRows(
    check({
      browser: { available: false, reason: 'The browser service did not answer.', checkedAt: '2026-11-22T10:00:00Z' },
      javaScript: { verdict: 'ProbablyNeeded', plain: { textLength: 84, linkCount: 1, emptyAppContainer: true }, rendered: null },
    }),
  )

  expect(textOf(rows, 'JavaScript')?.text).toContain('Probably needed.')
  expect(textOf(rows, 'JavaScript')?.text).toContain('Not confirmed: the browser service is not available.')
  expect(textOf(rows, 'Browser service')).toEqual({ name: 'Browser service', tone: 'err', text: 'Not available. The browser service did not answer.' })
})

it('only warns about a missing browser when the site does not need it', () => {
  const rows = checkRows(check({ browser: { available: false, reason: null, checkedAt: '2026-11-22T10:00:00Z' } }))

  expect(textOf(rows, 'Browser service')?.tone).toBe('warn')
})

it('shows a pending and a missing sitemap differently', () => {
  expect(textOf(checkRows(check({ sitemap: { state: 'Pending', files: 0, urls: 0, checkedAt: null } })), 'Sitemap')?.tone).toBe('pending')
  expect(textOf(checkRows(check({ sitemap: { state: 'None', files: 0, urls: 0, checkedAt: null } })), 'Sitemap')?.text).toContain('None found.')
  expect(textOf(checkRows(check({ sitemap: { state: 'Failed', files: 0, urls: 0, checkedAt: null } })), 'Sitemap')?.text).toContain('could not be read')
})

it('reports a start page that robots.txt or the network stopped', () => {
  const blocked = checkRows(check({ startPage: { statusCode: 0, reachable: false, blockedByRobots: true, problem: null }, robots: { startAllowed: false, crawlDelaySeconds: null, disallowed: [] } }))
  const failed = checkRows(check({ redirected: false, startPage: { statusCode: 0, reachable: false, blockedByRobots: false, problem: 'timeout' } }))

  expect(textOf(blocked, 'Start page')?.tone).toBe('err')
  expect(textOf(blocked, 'robots.txt')?.tone).toBe('err')
  expect(textOf(failed, 'Start page')?.text).toBe('timeout Nothing was fetched.')
  expect(textOf(failed, 'JavaScript')?.text).toBe('Not checked, because the start page could not be read.')
})

it('reports a status that is not 200', () => {
  const rows = checkRows(check({ redirected: false, startPage: { statusCode: 404, reachable: false, blockedByRobots: false, problem: null } }))

  expect(textOf(rows, 'Start page')).toEqual({ name: 'Start page', tone: 'err', text: 'docs.example.com answers 404. The crawl stays on docs.example.com.' })
})
