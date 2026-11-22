import { describe, expect, it } from 'vitest'
import type { TreeCountsDto } from '@/types/api'
import { processedOf, remainingOf, runStatus, type JobContext } from './run-status'
import { emptyCounts } from './tree-store'

const counts = (patch: Partial<TreeCountsDto>): TreeCountsDto => ({ ...emptyCounts(), ...patch })
const running: JobContext = { status: 'Running', pauseReason: null }

describe('runStatus', () => {
  it('names a folder that is being fetched, queued, in progress or done', () => {
    expect(runStatus(counts({ pages: 10, done: 3, fetching: 1, queued: 6 }), 'true', false, running).text).toBe('Fetching')
    expect(runStatus(counts({ pages: 10, queued: 10 }), 'true', false, running).text).toBe('Queued')
    expect(runStatus(counts({ pages: 10, done: 4, queued: 6 }), 'true', false, running).text).toBe('In progress')
    expect(runStatus(counts({ pages: 10, done: 10 }), 'true', false, running)).toEqual({ text: 'Done', tone: 'ok', live: false })
  })

  it('counts the failures of a finished folder', () => {
    expect(runStatus(counts({ pages: 1002, done: 0, failed: 1002 }), 'true', false, running)).toEqual({ text: '1,002 failed', tone: 'err', live: false })
  })

  it('separates unticked folders that were fetched before from those never fetched', () => {
    expect(runStatus(counts({ pages: 5, done: 2, stopped: 3 }), 'false', false, running).text).toBe('Stopped')
    expect(runStatus(counts({ pages: 5, stopped: 5 }), 'false', false, running).text).toBe('Not selected')
  })

  it('lets robots.txt win over everything', () => {
    expect(runStatus(counts({ pages: 50, skipped: 50 }), 'true', true, running).text).toBe('Blocked by robots.txt')
  })

  it('says why a paused job does not move', () => {
    const waiting = counts({ pages: 10, done: 4, queued: 6 })

    expect(runStatus(waiting, 'true', false, { status: 'Paused', pauseReason: 'browser-unavailable' }).text).toBe('Waiting for browser')
    expect(runStatus(waiting, 'true', false, { status: 'Paused', pauseReason: 'review' }).text).toBe('Paused')
    expect(runStatus(counts({ pages: 10, done: 10 }), 'true', false, { status: 'Paused', pauseReason: null }).text).toBe('Done')
  })

  it('adds up what was processed and what is left', () => {
    const sample = counts({ done: 3, failed: 2, queued: 4, fetching: 1 })

    expect([processedOf(sample), remainingOf(sample)]).toEqual([5, 5])
  })
})
