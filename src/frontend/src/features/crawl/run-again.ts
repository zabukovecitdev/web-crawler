import { formatNumber } from '@/lib/format'
import type { CrawlJobAction, CrawlJobDto } from '@/types/api'

export type RunAgainItem = { action: Extract<CrawlJobAction, 'retry-failed' | 'recrawl' | 'reprocess'>; label: string; description: string }

/** The ways to run a crawl again that apply to this job, in menu order. Empty when the job has not started or has no links. */
export function runAgainItems(job: Pick<CrawlJobDto, 'status' | 'links'>): RunAgainItem[] {
  if (job.status === 'Draft' || job.status === 'Pending' || job.links.total === 0) {
    return []
  }
  const items: RunAgainItem[] = []
  const failed = job.links.failed + job.links.retrying
  if (failed > 0) {
    items.push({
      action: 'retry-failed',
      label: `Retry ${formatNumber(failed)} failed ${failed === 1 ? 'link' : 'links'}`,
      description: 'Asks the site again for the links that failed. Pages already stored are not fetched.',
    })
  }
  items.push({
    action: 'recrawl',
    label: 'Fetch all links again',
    description: 'Asks the site for every link again, at its polite pace. Pages stay searchable meanwhile.',
  })
  if (job.status === 'Completed') {
    items.push({
      action: 'reprocess',
      label: 'Re-read stored pages',
      description: 'Parses and indexes stored pages again. Fetches only pages that were skipped.',
    })
  }
  return items
}
