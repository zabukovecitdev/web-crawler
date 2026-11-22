import { describe, expect, it } from 'vitest'
import type { CrawlJobDto, CrawlJobStatus } from '@/types/api'
import { groupFromParam, groupJobs, groupOf } from './crawl-groups'

function job(id: number, status: CrawlJobStatus, startedAt: string, completedAt: string | null = null): CrawlJobDto {
  return {
    id,
    status,
    host: `h${id}.example`,
    seedUrls: [],
    maxDepth: null,
    maxUrls: null,
    useJsRendering: false,
    renderMode: 'Auto',
    jsNeeded: false,
    pathRules: [],
    seedFromSitemap: false,
    followLinkedPages: true,
    reviewAfterPages: null,
    pauseReason: null,
    pauseDetail: null,
    createdAt: startedAt,
    startedAt,
    completedAt,
    links: { total: 0, queued: 0, fetching: 0, retrying: 0, parsing: 0, indexing: 0, indexed: 0, notIndexed: 0, skipped: 0, failed: 0, problems: [] },
  }
}

describe('groupOf', () => {
  it.each<[CrawlJobStatus, string]>([
    ['Running', 'active'],
    ['Paused', 'active'],
    ['Pending', 'active'],
    ['Draft', 'active'],
    ['Completed', 'finished'],
    ['Cancelled', 'finished'],
    ['Failed', 'finished'],
  ])('puts %s in %s', (status, group) => {
    expect(groupOf(status)).toBe(group)
  })
})

describe('groupJobs', () => {
  it('puts running crawls first even when an older job is running again', () => {
    const groups = groupJobs([
      job(9, 'Paused', '2026-11-16T10:00:00Z'),
      job(8, 'Completed', '2026-11-16T09:00:00Z', '2026-11-16T09:30:00Z'),
      job(3, 'Running', '2026-11-10T10:00:00Z'),
      job(7, 'Running', '2026-11-16T08:00:00Z'),
    ])
    expect(groups.active.map((j) => j.id)).toEqual([7, 3, 9])
    expect(groups.finished.map((j) => j.id)).toEqual([8])
  })

  it('orders finished crawls by when they finished, newest first', () => {
    const groups = groupJobs([
      job(1, 'Completed', '2026-11-01T00:00:00Z', '2026-11-15T00:00:00Z'),
      job(2, 'Failed', '2026-11-02T00:00:00Z', '2026-11-03T00:00:00Z'),
      job(3, 'Cancelled', '2026-11-04T00:00:00Z', '2026-11-05T00:00:00Z'),
    ])
    expect(groups.finished.map((j) => j.id)).toEqual([1, 3, 2])
  })
})

describe('groupFromParam', () => {
  const some = groupJobs([job(1, 'Running', '2026-11-16T00:00:00Z'), job(2, 'Completed', '2026-11-15T00:00:00Z', '2026-11-15T01:00:00Z')])
  const none = groupJobs([job(2, 'Completed', '2026-11-15T00:00:00Z', '2026-11-15T01:00:00Z')])

  it('keeps the group named in the URL', () => {
    expect(groupFromParam('finished', some)).toBe('finished')
    expect(groupFromParam('active', none)).toBe('active')
  })

  it('opens on crawls in progress when there are any, else on finished ones', () => {
    expect(groupFromParam(null, some)).toBe('active')
    expect(groupFromParam('bogus', none)).toBe('finished')
  })
})
