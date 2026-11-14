import type { ReactNode } from 'react'
import { Card } from '@/components/ui/card'
import type { CrawlProgress } from '@/types/api'
import { formatSeconds } from './crawl-format'
import { pathOf } from './crawl-rows'
import type { CurrentRequest } from './crawl-state'
import { useNow } from './useNow'

type Props = { progress: CrawlProgress; current: CurrentRequest | null }

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <p className="label text-graphite-foreground/75">{label}</p>
      <p className="readout m-0 text-graphite-foreground">{children}</p>
    </div>
  )
}

function stateOf(progress: CrawlProgress, current: CurrentRequest | null): string {
  if (progress.status !== 'Running') {
    return 'Idle'
  }
  return current ? 'Waiting for response' : 'Waiting for next request'
}

function sentAgo(current: CurrentRequest | null, now: number): string {
  return current ? `${formatSeconds(now - new Date(current.sentAt).getTime())} ago` : '—'
}

/** What the crawler is asking for right now, on the graphite card. */
export function CurrentRequestCard({ progress, current }: Props) {
  const now = useNow(progress.status === 'Running' && current !== null, 100)
  const url = current?.url ?? progress.currentUrl
  const sent = sentAgo(current, now)
  const depth = current?.depth ?? progress.currentDepth
  return (
    <Card className="border-0 bg-graphite p-5 text-graphite-foreground shadow-zb-1">
      <p className="label mb-4 text-graphite-foreground/75">Current request</p>
      <p className="readout m-0 break-all text-[15px] leading-[22px]">{url ? pathOf(url) : 'No request yet'}</p>
      <div className="mt-4 grid grid-cols-2 gap-4">
        <Field label="State">{stateOf(progress, current)}</Field>
        <Field label="Sent">{sent}</Field>
        <Field label="Depth">{depth ?? '—'}</Field>
        <Field label="In flight">{progress.inFlight}</Field>
      </div>
    </Card>
  )
}
