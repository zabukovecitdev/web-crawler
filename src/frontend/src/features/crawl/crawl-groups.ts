import type { CrawlJobDto, CrawlJobStatus } from '@/types/api'

export type CrawlGroup = 'active' | 'finished'

const ACTIVE_ORDER: Partial<Record<CrawlJobStatus, number>> = { Running: 0, Paused: 1, Pending: 2, Draft: 3 }

export function groupOf(status: CrawlJobStatus): CrawlGroup {
  return status in ACTIVE_ORDER ? 'active' : 'finished'
}

function time(value: string | null): number {
  return value ? new Date(value).getTime() : 0
}

function byActivity(a: CrawlJobDto, b: CrawlJobDto): number {
  const rank = (ACTIVE_ORDER[a.status] ?? 9) - (ACTIVE_ORDER[b.status] ?? 9)
  return rank || time(b.startedAt ?? b.createdAt) - time(a.startedAt ?? a.createdAt)
}

function byCompletion(a: CrawlJobDto, b: CrawlJobDto): number {
  return time(b.completedAt ?? b.createdAt) - time(a.completedAt ?? a.createdAt)
}

export function groupJobs(jobs: CrawlJobDto[]): Record<CrawlGroup, CrawlJobDto[]> {
  return {
    active: jobs.filter((job) => groupOf(job.status) === 'active').sort(byActivity),
    finished: jobs.filter((job) => groupOf(job.status) === 'finished').sort(byCompletion),
  }
}

export function groupFromParam(param: string | null, groups: Record<CrawlGroup, CrawlJobDto[]>): CrawlGroup {
  if (param === 'active' || param === 'finished') {
    return param
  }
  return groups.active.length > 0 ? 'active' : 'finished'
}
