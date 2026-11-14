import { useState } from 'react'
import { PageHeader } from '@/components/shared/PageHeader'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { RequestCrawlDialog } from '@/features/crawl/RequestCrawlDialog'
import { formatNumber } from '@/lib/format'
import { Pager } from './Pager'
import { SitesStats } from './SitesStats'
import { SitesTable } from './SitesTable'
import { useSites, type SiteSort } from './sites-api'
import { useDebounced } from './useDebounced'

const LIMIT = 25

function SitesBody({ filtered, onRequest }: { filtered: boolean; onRequest: () => void }) {
  if (filtered) {
    return <EmptyState title="No site matches this filter">Try another host name.</EmptyState>
  }
  return (
    <EmptyState title="No sites yet">
      A site appears here after its first crawl. Request a crawl to start.
      <span className="mt-4 block">
        <Button onClick={onRequest}>Request your first crawl</Button>
      </span>
    </EmptyState>
  )
}

type ListProps = {
  sites: ReturnType<typeof useSites>
  filtered: boolean
  sort: SiteSort
  offset: number
  onSort: (sort: SiteSort) => void
  onOffset: (offset: number) => void
  onRequest: () => void
}

function SitesList({ sites, filtered, sort, offset, onSort, onOffset, onRequest }: ListProps) {
  if (sites.isPending) {
    return <LoadingRows rows={6} />
  }
  if (sites.isError) {
    return <ErrorState error={sites.error} onRetry={() => void sites.refetch()} />
  }
  const data = sites.data
  if (data.items.length === 0) {
    return <SitesBody filtered={filtered} onRequest={onRequest} />
  }
  return (
    <>
      <SitesTable items={data.items} sort={sort} onSort={onSort} />
      <Pager total={data.total} offset={offset} limit={LIMIT} shown={data.items.length} onChange={onOffset} />
    </>
  )
}

export function SitesPage() {
  const [filter, setFilter] = useState('')
  const [sort, setSort] = useState<SiteSort>('lastCrawl')
  const [offset, setOffset] = useState(0)
  const [dialog, setDialog] = useState(false)
  const query = useDebounced(filter.trim())
  const sites = useSites({ query, sort, limit: LIMIT, offset })
  const data = sites.data

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        kicker={`Sites · ${formatNumber(data?.overview.sites ?? 0)}`}
        title="Sites"
        actions={
          <>
            <Input
              type="search"
              aria-label="Filter by host"
              placeholder="Filter by host"
              value={filter}
              onChange={(e) => {
                setFilter(e.target.value)
                setOffset(0)
              }}
              className="w-full sm:w-60"
            />
            <Button onClick={() => setDialog(true)}>Request crawl</Button>
          </>
        }
      />
      {data ? <SitesStats overview={data.overview} items={data.items} /> : null}
      <Card className="p-6 shadow-zb-1">
        <h2 className="mb-5 mt-0 text-lg font-semibold leading-6">All sites</h2>
        <SitesList
          sites={sites}
          filtered={query !== ''}
          sort={sort}
          offset={offset}
          onSort={(next) => {
            setSort(next)
            setOffset(0)
          }}
          onOffset={setOffset}
          onRequest={() => setDialog(true)}
        />
      </Card>
      <RequestCrawlDialog open={dialog} onOpenChange={setDialog} />
    </div>
  )
}
