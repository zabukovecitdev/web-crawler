import { describe, expect, it } from 'vitest'
import type { JobLinkSummary } from '@/types/api'
import { jobHeadline, jobSteps, pendingLinks, problemsText } from './link-format'

const NONE: JobLinkSummary = { total: 0, queued: 0, fetching: 0, retrying: 0, parsing: 0, indexing: 0, indexed: 0, notIndexed: 0, skipped: 0, failed: 0, problems: [] }

describe('jobSteps', () => {
  it('counts what passed each step', () => {
    const links = { ...NONE, total: 10, queued: 2, parsing: 1, indexing: 1, indexed: 3, notIndexed: 1, skipped: 1, failed: 1 }
    const steps = jobSteps(links)
    expect(steps.map((step) => [step.done, step.of])).toEqual([[8, 10], [6, 6], [5, 6], [3, 4]])
    expect(steps.map((step) => step.status)).toEqual(['active', 'done', 'active', 'active'])
  })

  it('keeps Store active while nothing storable has been stored yet', () => {
    const steps = jobSteps({ ...NONE, total: 4, queued: 2, fetching: 1, failed: 1 })
    expect(steps[1]).toMatchObject({ done: 0, of: 0, status: 'active' })
  })

  it('shows Store done at 100 % when all storable pages are stored and some links were skipped', () => {
    const steps = jobSteps({ ...NONE, total: 5, indexed: 3, skipped: 2 })
    expect(steps[1]).toMatchObject({ done: 3, of: 3, status: 'done' })
  })

  it('is done when every link went through', () => {
    const steps = jobSteps({ ...NONE, total: 3, indexed: 2, failed: 1 })
    expect(steps.map((step) => step.status)).toEqual(['done', 'done', 'done', 'done'])
  })

  it('waits on Store when nothing was fetched', () => {
    expect(jobSteps({ ...NONE, total: 2, queued: 2 })[1]).toMatchObject({ done: 0, of: 0, status: 'waiting' })
  })

  it('waits before the first fetch', () => {
    expect(jobSteps({ ...NONE, total: 1, queued: 1 }).map((step) => step.status)).toEqual(['waiting', 'waiting', 'waiting', 'waiting'])
  })
})

describe('jobHeadline', () => {
  it('says what a running job does now', () => {
    expect(jobHeadline({ status: 'Running', links: { ...NONE, total: 5, queued: 3 } })).toBe('Fetching: 3 links left in the queue.')
    expect(jobHeadline({ status: 'Running', links: { ...NONE, total: 5, parsing: 1 } })).toBe('Parsing 1 stored page.')
  })

  it('names the failures of a finished job', () => {
    expect(jobHeadline({ status: 'Completed', links: { ...NONE, total: 5, indexed: 4, failed: 1 } })).toBe(
      'Finished, but 1 link failed. 4 pages in the search index.',
    )
  })
})

describe('summary helpers', () => {
  it('lists the problems with their counts', () => {
    const problems = [{ reason: 'http-404', label: 'HTTP 404', count: 2 }, { reason: 'timeout', label: 'Timed out', count: 1 }]
    expect(problemsText({ ...NONE, problems })).toBe('HTTP 404 × 2, Timed out × 1')
  })

  it('counts the links that still move', () => {
    expect(pendingLinks({ ...NONE, queued: 1, fetching: 1, retrying: 1, parsing: 1, indexing: 1, indexed: 9 })).toBe(5)
  })
})
