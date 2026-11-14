import type { ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'

export function EmptyState({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="flex flex-col items-center gap-2 px-4 py-12 text-center">
      <p className="label">Nothing here</p>
      <p className="m-0 text-lg font-semibold">{title}</p>
      {children ? <p className="max-w-md text-[13px] text-muted-foreground">{children}</p> : null}
    </div>
  )
}

type ErrorProps = { error: unknown; onRetry?: () => void }

export function ErrorState({ error, onRetry }: ErrorProps) {
  const message = error instanceof Error ? error.message : 'Something went wrong.'
  return (
    <div role="alert" className="flex flex-col items-start gap-3 px-4 py-8">
      <p className="m-0 text-[13px] leading-[18px] text-signal-ink">
        <strong>Error:</strong> {message}
      </p>
      {onRetry ? (
        <Button variant="outline" size="sm" onClick={onRetry}>
          Try again
        </Button>
      ) : null}
    </div>
  )
}

export function LoadingRows({ rows = 5 }: { rows?: number }) {
  return (
    <div className="flex flex-col gap-3 p-1" role="status" aria-label="Loading">
      {Array.from({ length: rows }, (_, i) => (
        <Skeleton key={i} className="h-5 w-full" />
      ))}
    </div>
  )
}
