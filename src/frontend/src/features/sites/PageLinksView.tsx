import { useState } from 'react'
import { Link } from 'react-router-dom'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { StatusBadge } from '@/components/shared/StatusBadge'
import { Badge } from '@/components/ui/badge'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { PageLinkDirection, PageLinkDto } from '@/types/api'
import { Pager } from './Pager'
import { linkTarget } from './link-target'
import { LINKS_PAGE_SIZE, usePageLinks } from './pages-api'
import { LINK, TD, TH } from './table-styles'

const SUB_TABS: { value: PageLinkDirection; label: string }[] = [
  { value: 'out', label: 'Links on this page' },
  { value: 'in', label: 'Linked from' },
]

const EMPTY: Record<PageLinkDirection, string> = {
  out: 'No links stored for this page. Links are stored when a page is parsed.',
  in: 'No page links here yet.',
}

function UrlCell({ link }: { link: PageLinkDto }) {
  const target = linkTarget(link.url, link.pageId)
  if (target.kind === 'app') {
    return (
      <Link className={LINK} to={target.to}>
        {link.url}
      </Link>
    )
  }
  if (target.kind === 'external') {
    return (
      <a className={LINK} href={target.href} target="_blank" rel="noopener noreferrer">
        {link.url}
      </a>
    )
  }
  return <span className="readout [overflow-wrap:anywhere]">{link.url}</span>
}

function StatusCell({ code }: { code: number | null }) {
  return code === null ? <span className="text-muted-foreground">Not crawled</span> : <StatusBadge code={code} />
}

function AnchorCell({ text }: { text: string | null }) {
  return text ? <span>{text}</span> : <span className="text-muted-foreground">No text</span>
}

function OutRow({ link }: { link: PageLinkDto }) {
  return (
    <TableRow>
      <TableCell className={`${TD} max-w-[320px]`}>
        <UrlCell link={link} />
      </TableCell>
      <TableCell className={TD}>
        <AnchorCell text={link.text} />
      </TableCell>
      <TableCell className={`${TD} whitespace-nowrap`}>
        <Badge variant="neutral">{link.internal ? 'Internal' : 'External'}</Badge>
        {link.noFollow ? <span className="label ml-2">nofollow</span> : null}
      </TableCell>
      <TableCell className={`${TD} whitespace-nowrap`}>
        <StatusCell code={link.statusCode} />
      </TableCell>
    </TableRow>
  )
}

function InRow({ link }: { link: PageLinkDto }) {
  return (
    <TableRow>
      <TableCell className={`${TD} max-w-[320px]`}>
        <UrlCell link={link} />
      </TableCell>
      <TableCell className={TD}>
        <AnchorCell text={link.text} />
      </TableCell>
      <TableCell className={`${TD} whitespace-nowrap`}>
        <StatusCell code={link.statusCode} />
      </TableCell>
    </TableRow>
  )
}

function LinksTable({ direction, items }: { direction: PageLinkDirection; items: PageLinkDto[] }) {
  const out = direction === 'out'
  return (
    <Table className="min-w-[560px]">
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead scope="col" className={TH}>{out ? 'Target' : 'Linking page'}</TableHead>
          <TableHead scope="col" className={TH}>Anchor text</TableHead>
          {out ? <TableHead scope="col" className={TH}>Type</TableHead> : null}
          <TableHead scope="col" className={TH}>Status</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((link, i) =>
          out ? <OutRow key={i} link={link} /> : <InRow key={i} link={link} />,
        )}
      </TableBody>
    </Table>
  )
}

function LinksList({ id, direction }: { id: number; direction: PageLinkDirection }) {
  const [offset, setOffset] = useState(0)
  const links = usePageLinks(id, direction, offset)
  if (links.isPending) {
    return <LoadingRows rows={6} />
  }
  if (links.isError) {
    return <ErrorState error={links.error} onRetry={() => void links.refetch()} />
  }
  if (links.data.total === 0) {
    return <EmptyState title="No links">{EMPTY[direction]}</EmptyState>
  }
  return (
    <div>
      <div className="overflow-x-auto">
        <LinksTable direction={direction} items={links.data.items} />
      </div>
      <Pager
        total={links.data.total}
        offset={offset}
        limit={LINKS_PAGE_SIZE}
        shown={links.data.items.length}
        onChange={setOffset}
      />
    </div>
  )
}

function SubTabs({ value, onChange }: { value: PageLinkDirection; onChange: (v: PageLinkDirection) => void }) {
  return (
    <nav aria-label="Link direction" className="mb-4 flex flex-wrap gap-2">
      {SUB_TABS.map((tab) => (
        <button
          key={tab.value}
          type="button"
          aria-current={tab.value === value ? 'true' : undefined}
          onClick={() => onChange(tab.value)}
          className="label rounded-sm border border-border px-3 py-1.5 aria-[current=true]:border-foreground aria-[current=true]:bg-muted aria-[current=true]:text-foreground"
        >
          {tab.label}
        </button>
      ))}
    </nav>
  )
}

export function PageLinksView({ id }: { id: number }) {
  const [direction, setDirection] = useState<PageLinkDirection>('out')
  return (
    <div>
      <SubTabs value={direction} onChange={setDirection} />
      <LinksList key={`${id}-${direction}`} id={id} direction={direction} />
    </div>
  )
}
