import { Link } from 'react-router-dom'
import { ToneBadge } from '@/components/shared/StatusBadge'
import { Button } from '@/components/ui/button'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { JobLinkDto } from '@/types/api'
import { LINK, TD, TH } from '../sites/table-styles'
import { formatClock } from './crawl-format'
import { linkLook } from './link-format'

type RowProps = { link: JobLinkDto; busy: boolean; disabledReason?: string; onRecrawl: (link: JobLinkDto) => void }

function pathOf(url: string): string {
  try {
    const parsed = new URL(url)
    return parsed.pathname + parsed.search
  } catch {
    return url
  }
}

function retryNote(link: JobLinkDto): string | null {
  return link.state === 'Retrying' && link.nextCrawlAt ? ` Next try at ${formatClock(link.nextCrawlAt)}.` : null
}

function LinkRow({ link, busy, disabledReason, onRecrawl }: RowProps) {
  const look = linkLook(link.state)
  return (
    <TableRow>
      <TableCell className={`${TD} whitespace-nowrap`}>
        <ToneBadge tone={look.tone} live={look.live}>
          {look.word}
        </ToneBadge>
      </TableCell>
      <TableCell className={TD}>
        <Link className={LINK} to={`/sites/${encodeURIComponent(link.host)}/pages/${link.id}`}>
          {link.title || pathOf(link.url)}
        </Link>
        <div className="readout text-muted-foreground [overflow-wrap:anywhere]">{link.url}</div>
      </TableCell>
      <TableCell className={`${TD} min-w-[220px] text-[13px] leading-[18px]`}>
        {link.reason}
        {retryNote(link)}
      </TableCell>
      <TableCell className={`${TD} whitespace-nowrap text-right`}>
        <Button
          variant="outline"
          size="sm"
          title={disabledReason}
          disabled={busy || !link.canRecrawl || disabledReason !== undefined}
          onClick={() => onRecrawl(link)}>
          Recrawl
        </Button>
      </TableCell>
    </TableRow>
  )
}

type Props = { links: JobLinkDto[]; busy: boolean; disabledReason?: string; onRecrawl: (link: JobLinkDto) => void }

/** One row for each link: its state, the page it leads to, the reason in words, and a recrawl. */
export function JobLinksTable({ links, busy, disabledReason, onRecrawl }: Props) {
  return (
    <Table className="min-w-[860px]">
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead scope="col" className={TH}>State</TableHead>
          <TableHead scope="col" className={TH}>Link</TableHead>
          <TableHead scope="col" className={TH}>Why</TableHead>
          <TableHead scope="col" className={TH}>
            <span className="sr-only">Actions</span>
          </TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {links.map((link) => (
          <LinkRow key={link.id} link={link} busy={busy} disabledReason={disabledReason} onRecrawl={onRecrawl} />
        ))}
      </TableBody>
    </Table>
  )
}
