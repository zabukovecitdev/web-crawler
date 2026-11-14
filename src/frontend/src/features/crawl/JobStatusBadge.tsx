import { ToneBadge } from '@/components/shared/StatusBadge'
import type { CrawlJobStatus } from '@/types/api'
import { jobLook } from './crawl-format'

/** Job status as a word, with the LED only while the crawl runs. */
export function JobStatusBadge({ status }: { status: CrawlJobStatus }) {
  const look = jobLook(status)
  return (
    <ToneBadge tone={look.tone} live={look.live}>
      {look.word}
    </ToneBadge>
  )
}
