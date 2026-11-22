import { ToneBadge } from '@/components/shared/StatusBadge'
import type { CrawlJobStatus, PauseReason } from '@/types/api'
import { jobLook } from './crawl-format'

export function JobStatusBadge({ status, pauseReason = null }: { status: CrawlJobStatus; pauseReason?: PauseReason | null }) {
  const look = jobLook(status, pauseReason)
  return (
    <ToneBadge tone={look.tone} live={look.live}>
      {look.word}
    </ToneBadge>
  )
}
