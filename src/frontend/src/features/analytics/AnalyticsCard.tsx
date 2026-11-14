import type { ReactNode } from 'react'
import { ErrorState, LoadingRows } from '@/components/shared/States'
import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'

type Props = { kicker?: string; title: string; children: ReactNode; className?: string }

/** Card with an engraved kicker and an h2 title, shared by every analytics panel. */
export function AnalyticsCard({ kicker, title, children, className }: Props) {
  return (
    <Card className={cn('flex min-w-0 flex-col gap-4 p-5 shadow-zb-1', className)}>
      <div className="flex flex-col gap-1">
        {kicker ? <p className="label">{kicker}</p> : null}
        <h2 className="m-0 text-lg font-semibold leading-6">{title}</h2>
      </div>
      {children}
    </Card>
  )
}

type Query = { isPending: boolean; isError: boolean; error: unknown; refetch: () => unknown }

/** Renders loading / error, or the children once data is there. */
export function PanelState({ query, children }: { query: Query; children: ReactNode }) {
  if (query.isPending) {
    return <LoadingRows rows={4} />
  }
  if (query.isError) {
    return <ErrorState error={query.error} onRetry={() => void query.refetch()} />
  }
  return <>{children}</>
}

export function EmptyPanel() {
  return <p className="m-0 py-6 text-[13px] text-muted-foreground">No requests in this range</p>
}
