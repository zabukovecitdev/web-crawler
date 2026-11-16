import type { ReactNode } from 'react'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { errorMessage } from '@/lib/api'
import type { PageContent } from './pages-api'
import { isForbidden } from './sites-api'

type Query = {
  isPending: boolean
  isError: boolean
  error: unknown
  data?: PageContent
  refetch: () => unknown
}

type Props = {
  query: Query
  /** Shown above the body when the stored source was cut at the size limit. */
  truncatedNote?: string
  children: (data: PageContent) => ReactNode
}

const DEFAULT_NOTE = 'The stored source is longer than the dashboard shows. This is the first part.'

function StoredBodyError({ query }: { query: Query }) {
  if (isForbidden(query.error)) {
    return <EmptyState title="Stored copy hidden">{errorMessage(query.error)}</EmptyState>
  }
  return <ErrorState error={query.error} onRetry={() => void query.refetch()} />
}

/** The loading, hidden, error and truncated states that every view of the stored body shares. */
export function StoredBodyState({ query, truncatedNote = DEFAULT_NOTE, children }: Props) {
  if (query.isError) {
    return <StoredBodyError query={query} />
  }
  if (query.isPending || !query.data) {
    return <LoadingRows rows={6} />
  }
  return (
    <div className="flex flex-col gap-3">
      {query.data.truncated ? (
        <p role="note" className="m-0 text-[13px] text-muted-foreground">
          {truncatedNote}
        </p>
      ) : null}
      {children(query.data)}
    </div>
  )
}
