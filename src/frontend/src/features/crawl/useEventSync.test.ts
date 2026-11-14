import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { CrawlProgress } from '@/types/api'
import type { CrawlAction } from './crawl-state'
import { POLL_MS, RECONCILE_MS, syncInterval, syncOnce } from './useEventSync'

const api = vi.hoisted(() => ({ fetchProgress: vi.fn(), fetchEventsSince: vi.fn() }))
vi.mock('./crawl-api', () => api)

it('polls every 3 s unless live, then reconciles every 30 s', () => {
  expect(syncInterval('connecting')).toBe(POLL_MS)
  expect(syncInterval('reconnecting')).toBe(POLL_MS)
  expect(syncInterval('offline')).toBe(POLL_MS)
  expect(syncInterval('live')).toBe(RECONCILE_MS)
})

describe('syncOnce', () => {
  beforeEach(() => vi.clearAllMocks())

  it('fetches from the cursor and applies history before the snapshot', async () => {
    api.fetchProgress.mockResolvedValue({ jobId: 3 } as unknown as CrawlProgress)
    api.fetchEventsSince.mockResolvedValue([{ id: 8 }])
    const actions: CrawlAction[] = []
    await syncOnce(3, 7, (a) => actions.push(a))
    expect(api.fetchEventsSince).toHaveBeenCalledWith(3, 7)
    expect(actions.map((a) => a.type)).toEqual(['history', 'snapshot'])
  })

  it('dispatches nothing when a request fails', async () => {
    api.fetchProgress.mockRejectedValue(new Error('down'))
    api.fetchEventsSince.mockResolvedValue([])
    const actions: CrawlAction[] = []
    await expect(syncOnce(3, 0, (a) => actions.push(a))).rejects.toThrow('down')
    expect(actions).toEqual([])
  })
})
