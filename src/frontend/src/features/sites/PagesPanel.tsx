import { useState } from 'react'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatNumber } from '@/lib/format'
import type { SiteDetail } from '@/types/api'
import { Pager } from './Pager'
import { PagesTable } from './PagesTable'
import { useHostPages, type PageState } from './pages-api'
import { useDebounced } from './useDebounced'

const LIMIT = 25

function countRange(site: SiteDetail, from: number, to: number): number {
  return site.statusCodes.filter((s) => s.statusCode >= from && s.statusCode < to).reduce((sum, s) => sum + s.count, 0)
}

function StateTabs({ site, value, onChange }: { site: SiteDetail; value: PageState; onChange: (v: PageState) => void }) {
  const counts: [PageState, string, number][] = [
    ['all', 'All', site.pages],
    ['ok', 'OK', countRange(site, 200, 300)],
    ['redirect', 'Redirects', countRange(site, 300, 400)],
    ['error', 'Errors', countRange(site, 400, 600)],
  ]
  return (
    <Tabs value={value} onValueChange={(v) => onChange(v as PageState)}>
      <TabsList aria-label="Status filter" className="h-auto flex-wrap">
        {counts.map(([key, label, count]) => (
          <TabsTrigger key={key} value={key} className="text-[13px]">
            {label} {formatNumber(count)}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  )
}

export function PagesPanel({ host, site }: { host: string; site: SiteDetail }) {
  const [search, setSearch] = useState('')
  const [state, setState] = useState<PageState>('all')
  const [offset, setOffset] = useState(0)
  const query = useDebounced(search.trim())
  const pages = useHostPages(host, { query, state, limit: LIMIT, offset })
  const data = pages.data

  return (
    <Card className="p-6 shadow-zb-1">
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <Input
          type="search"
          aria-label="Filter by URL or title"
          placeholder="Filter by URL or title"
          value={search}
          onChange={(e) => {
            setSearch(e.target.value)
            setOffset(0)
          }}
          className="w-full sm:max-w-[360px]"
        />
        <StateTabs site={site} value={state} onChange={(v) => { setState(v); setOffset(0) }} />
      </div>
      {pages.isPending ? <LoadingRows rows={8} /> : null}
      {pages.isError ? <ErrorState error={pages.error} onRetry={() => void pages.refetch()} /> : null}
      {data && data.items.length === 0 ? (
        <EmptyState title="No pages match">Change the filter or wait for the crawl to reach more pages.</EmptyState>
      ) : null}
      {data && data.items.length > 0 ? (
        <>
          <PagesTable host={host} items={data.items} />
          <Pager total={data.total} offset={offset} limit={LIMIT} shown={data.items.length} onChange={setOffset} />
        </>
      ) : null}
    </Card>
  )
}
