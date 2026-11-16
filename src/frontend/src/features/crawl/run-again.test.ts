import { describe, expect, it } from 'vitest'
import type { CrawlJobDto, JobLinkSummary } from '@/types/api'
import { runAgainItems } from './run-again'

const NONE: JobLinkSummary = { total: 0, queued: 0, fetching: 0, retrying: 0, parsing: 0, indexing: 0, indexed: 0, notIndexed: 0, skipped: 0, failed: 0, problems: [] }

function actions(status: CrawlJobDto['status'], links: Partial<JobLinkSummary>) {
  return runAgainItems({ status, links: { ...NONE, ...links } }).map((item) => item.action)
}

describe('runAgainItems', () => {
  it.each([
    ['Running', { total: 5, indexed: 5 }, ['recrawl']],
    ['Paused', { total: 5, indexed: 5 }, ['recrawl']],
    ['Completed', { total: 5, indexed: 5 }, ['recrawl', 'reprocess']],
    ['Cancelled', { total: 5, indexed: 5 }, ['recrawl']],
    ['Failed', { total: 5, indexed: 5 }, ['recrawl']],
    ['Running', { total: 5, indexed: 4, failed: 1 }, ['retry-failed', 'recrawl']],
    ['Paused', { total: 5, indexed: 4, retrying: 1 }, ['retry-failed', 'recrawl']],
    ['Completed', { total: 5, indexed: 3, failed: 1, retrying: 1 }, ['retry-failed', 'recrawl', 'reprocess']],
    ['Cancelled', { total: 5, indexed: 4, failed: 1 }, ['retry-failed', 'recrawl']],
    ['Failed', { total: 5, indexed: 4, failed: 1 }, ['retry-failed', 'recrawl']],
  ] as const)('%s with %j offers %j', (status, links, expected) => {
    expect(actions(status, links)).toEqual(expected)
  })

  it('offers nothing before the job started or without links', () => {
    expect(actions('Draft', { total: 3, queued: 3 })).toEqual([])
    expect(actions('Pending', { total: 3, queued: 3 })).toEqual([])
    expect(actions('Running', {})).toEqual([])
  })

  it('counts failed and retrying links in the retry label', () => {
    const [retry] = runAgainItems({ status: 'Running', links: { ...NONE, total: 4, failed: 2, retrying: 1 } })
    expect(retry.label).toBe('Retry 3 failed links')
  })
})
