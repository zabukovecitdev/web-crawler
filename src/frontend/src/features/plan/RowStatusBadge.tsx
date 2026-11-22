import { Badge } from '@/components/ui/badge'
import { Led } from '@/components/shared/Led'
import type { RowStatus, StatusTone } from './run-status'

const VARIANT = {
  live: 'success',
  warn: 'warning',
  err: 'signal',
  ok: 'success',
  info: 'outline',
  mute: 'neutral',
} as const satisfies Record<StatusTone, 'success' | 'warning' | 'signal' | 'outline' | 'neutral'>

export function RowStatusBadge({ status }: { status: RowStatus | null }) {
  if (!status) {
    return null
  }
  return (
    <Badge variant={VARIANT[status.tone]}>
      {status.live ? <Led /> : null}
      {status.text}
    </Badge>
  )
}
