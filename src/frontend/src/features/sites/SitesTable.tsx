import { Link } from 'react-router-dom'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatMs, formatNumber, formatRelative } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { SiteSummary } from '@/types/api'
import { LINK, TD, TD_N, TH, TH_R } from './table-styles'
import { SiteStatusBadge } from './SiteStatusBadge'
import type { SiteSort } from './sites-api'

type Props = { items: SiteSummary[]; sort: SiteSort; onSort: (sort: SiteSort) => void }

type SortHeadProps = { label: string; field: SiteSort; sort: SiteSort; right?: boolean; onSort: (sort: SiteSort) => void }

function SortHead({ label, field, sort, right, onSort }: SortHeadProps) {
  const active = sort === field
  return (
    <TableHead scope="col" className={right ? TH_R : TH} aria-sort={active ? 'descending' : undefined}>
      <button
        type="button"
        onClick={() => onSort(field)}
        className={cn('font-[inherit] uppercase tracking-[inherit] hover:text-foreground', active && 'text-foreground')}
      >
        {label}
        {active ? <span aria-hidden="true"> ↓</span> : null}
      </button>
    </TableHead>
  )
}

function SiteRow({ site }: { site: SiteSummary }) {
  return (
    <TableRow>
      <TableCell className={TD}>
        <Link className={LINK} to={`/sites/${encodeURIComponent(site.host)}`}>
          {site.host}
        </Link>
      </TableCell>
      <TableCell className={TD}>
        <SiteStatusBadge status={site.status} />
      </TableCell>
      <TableCell className={TD_N}>{formatNumber(site.pages)}</TableCell>
      <TableCell className={TD_N}>{formatNumber(site.failed)}</TableCell>
      <TableCell className={TD_N}>{formatMs(site.avgResponseMs)}</TableCell>
      <TableCell className={TD_N}>{formatMs(site.crawlDelayMs)}</TableCell>
      <TableCell className={cn(TD, 'whitespace-nowrap text-muted-foreground')}>{formatRelative(site.lastCrawlAt)}</TableCell>
    </TableRow>
  )
}

export function SitesTable({ items, sort, onSort }: Props) {
  const head = { sort, onSort }
  return (
    <Table className="min-w-[760px]">
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <SortHead label="Host" field="host" {...head} />
          <TableHead scope="col" className={TH}>
            Status
          </TableHead>
          <SortHead label="Pages" field="pages" right {...head} />
          <SortHead label="Failed" field="failed" right {...head} />
          <TableHead scope="col" className={TH_R}>
            Median response
          </TableHead>
          <TableHead scope="col" className={TH_R}>
            Delay
          </TableHead>
          <SortHead label="Last crawl" field="lastCrawl" {...head} />
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((site) => (
          <SiteRow key={site.host} site={site} />
        ))}
      </TableBody>
    </Table>
  )
}
