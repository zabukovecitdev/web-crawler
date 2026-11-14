import { useEffect, useState, type FormEvent } from 'react'
import { PageHeader } from '@/components/shared/PageHeader'
import { EmptyState, ErrorState } from '@/components/shared/States'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { formatNumber } from '@/lib/format'
import type { SearchResponse } from '@/types/api'
import { Facets } from './Facets'
import { Results, ResultsSkeleton } from './Results'
import { canSearch, MIN_QUERY_LENGTH, useSearch, type SearchParams } from './search-api'
import { useSearchState } from './useSearchState'

type Update = (patch: Partial<SearchParams>) => void

function Summary({ data }: { data: SearchResponse }) {
  const sites = data.hosts.length
  return (
    <p className="readout m-0 text-muted-foreground" aria-live="polite">
      {formatNumber(data.total)} {data.total === 1 ? 'result' : 'results'} · {formatNumber(data.tookMs)} ms ·{' '}
      {formatNumber(sites)} {sites === 1 ? 'site' : 'sites'}
    </p>
  )
}

function Loaded({ data, state, update }: { data: SearchResponse; state: SearchParams; update: Update }) {
  return (
    <>
      <Summary data={data} />
      <div className="grid gap-6 lg:grid-cols-[minmax(0,3fr)_minmax(0,1fr)]">
        <div className="order-2 lg:order-1">
          {data.items.length > 0 ? (
            <Results items={data.items} total={data.total} page={state.page} onPage={(page) => update({ page })} />
          ) : (
            <EmptyState title="No results on this page">Go back to the first page or pick another site.</EmptyState>
          )}
        </div>
        <div className="order-1 lg:order-2">
          <Facets hosts={data.hosts} selected={state.host} onSelect={(host) => update({ host, page: 1 })} />
        </div>
      </div>
    </>
  )
}

function Body({ state, update }: { state: SearchParams; update: Update }) {
  const search = useSearch(state)
  const data = search.data
  if (!canSearch(state.q)) {
    return (
      <EmptyState title="Search every crawled page">
        Type at least {MIN_QUERY_LENGTH} characters and press Enter. Results come from all indexed sites.
      </EmptyState>
    )
  }
  if (search.isError) {
    return <ErrorState error={search.error} onRetry={() => void search.refetch()} />
  }
  if (!data) {
    return <ResultsSkeleton />
  }
  if (data.hosts.length === 0) {
    return (
      <EmptyState title={`No results for “${state.q.trim()}”`}>
        Check the spelling, try fewer words, or crawl more sites.
      </EmptyState>
    )
  }
  return <Loaded data={data} state={state} update={update} />
}

export function SearchPage() {
  const { state, update } = useSearchState()
  const [draft, setDraft] = useState(state.q)
  useEffect(() => setDraft(state.q), [state.q])

  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (canSearch(draft)) {
      update({ q: draft.trim(), host: '', page: 1 })
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader kicker="All sites" title="Search" />
      <form role="search" onSubmit={submit} className="flex max-w-[720px] gap-3">
        <Input
          type="search"
          aria-label="Search crawled content"
          placeholder="Search crawled content"
          className="min-w-0 flex-1"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <Button type="submit" disabled={!canSearch(draft)}>
          Search
        </Button>
      </form>
      <Body state={state} update={update} />
    </div>
  )
}
