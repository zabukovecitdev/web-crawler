import { ToneBadge } from '@/components/shared/StatusBadge'
import type { SiteStatus } from '@/types/api'
import { SITE_STATUS } from './labels'

export function SiteStatusBadge({ status }: { status: SiteStatus }) {
  const look = SITE_STATUS[status]
  return (
    <ToneBadge tone={look.tone} live={look.live}>
      {look.word}
    </ToneBadge>
  )
}
