import { ToneBadge } from '@/components/shared/StatusBadge'
import type { CrawlJobStatus } from '@/types/api'
import { jobLook } from './crawl-format'

export function JobStatusBadge({ status }: { status: CrawlJobStatus }) {
  const look = jobLook(status)
  return (
    <ToneBadge tone={look.tone} live={look.live}>
      {look.word}
    </ToneBadge>
  )
}
