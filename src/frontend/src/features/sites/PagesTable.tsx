import { Link } from 'react-router-dom'
import { StatusBadge } from '@/components/shared/StatusBadge'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatBytes, formatDate, formatMs, formatNumber } from '@/lib/format'
import type { PageSummary } from '@/types/api'
import { CONTENT_LABELS } from './labels'
import { LINK, TD, TD_N, TH, TH_R } from './table-styles'

function PageRow({ host, page }: { host: string; page: PageSummary }) {
  return (
    <TableRow>
      <TableCell className={TD}>
        <Link className={LINK} to={`/sites/${encodeURIComponent(host)}/pages/${page.id}`}>
          {page.title || page.path}
        </Link>
        <div className="readout text-muted-foreground [overflow-wrap:anywhere]">{page.path}</div>
      </TableCell>
      <TableCell className={`${TD} whitespace-nowrap`}>
        <StatusBadge code={page.statusCode} />
      </TableCell>
      <TableCell className={`${TD} readout`}>{page.contentType ?? '—'}</TableCell>
      <TableCell className={TD_N}>{formatBytes(page.contentLength)}</TableCell>
      <TableCell className={TD_N}>{formatMs(page.responseTimeMs)}</TableCell>
      <TableCell className={TD_N}>{formatNumber(page.depth)}</TableCell>
      <TableCell className={`${TD} whitespace-nowrap text-muted-foreground`}>{formatDate(page.lastCrawlAt)}</TableCell>
      <TableCell className={TD}>{CONTENT_LABELS[page.contentState]}</TableCell>
    </TableRow>
  )
}

export function PagesTable({ host, items }: { host: string; items: PageSummary[] }) {
  return (
    <Table className="min-w-[920px]">
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead scope="col" className={TH}>Page</TableHead>
          <TableHead scope="col" className={TH}>Status</TableHead>
          <TableHead scope="col" className={TH}>Type</TableHead>
          <TableHead scope="col" className={TH_R}>Size</TableHead>
          <TableHead scope="col" className={TH_R}>Response</TableHead>
          <TableHead scope="col" className={TH_R}>Depth</TableHead>
          <TableHead scope="col" className={TH}>Fetched</TableHead>
          <TableHead scope="col" className={TH}>Content</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((page) => (
          <PageRow key={page.id} host={host} page={page} />
        ))}
      </TableBody>
    </Table>
  )
}
