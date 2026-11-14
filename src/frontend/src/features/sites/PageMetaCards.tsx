import { Link } from 'react-router-dom'
import { StatusBadge } from '@/components/shared/StatusBadge'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatBytes, formatDate, formatMs, formatNumber } from '@/lib/format'
import type { PageDetail, ParsedPageDto } from '@/types/api'
import { CONTENT_LABELS } from './labels'
import { KeyValues } from './KeyValues'
import { Panel } from './Panel'
import { TD, TD_N, TH, TH_R } from './table-styles'

const fullDate = new Intl.DateTimeFormat('en-GB', { dateStyle: 'medium', timeStyle: 'medium' })
const when = (iso: string | null): string => (iso ? fullDate.format(new Date(iso)) : '—')

export function CrawlMetadataCard({ page }: { page: PageDetail }) {
  const last = page.fetches[0]
  return (
    <Panel title="Crawl metadata">
      <KeyValues
        rows={[
          ['Status', <StatusBadge key="s" code={page.statusCode} />],
          ['Fetched', when(page.lastCrawlAt)],
          ['Response time', formatMs(page.responseTimeMs)],
          ['Content type', page.contentType ?? '—'],
          ['Size', formatBytes(page.contentLength)],
          ['Depth', formatNumber(page.depth)],
          ['Final URL', last?.finalUrl ?? page.url],
          ['Rendering', page.usedJsRendering ? 'Rendered with JavaScript' : 'HTTP, no JavaScript'],
          [
            'Crawl job',
            page.crawlJobId ? <Link key="j" to={`/crawls/${page.crawlJobId}`}>#{page.crawlJobId}</Link> : '—',
          ],
          ['Next crawl', when(page.nextCrawlAt)],
          ['Content', CONTENT_LABELS[page.contentState]],
          ...(page.failReason ? ([['Failure', page.failReason]] as [string, string][]) : []),
        ]}
      />
    </Panel>
  )
}

function structured(parsed: ParsedPageDto): string {
  const n = parsed.jsonLdBlocks
  return n === 0 ? 'None' : `${n} JSON-LD block${n === 1 ? '' : 's'}`
}

export function PageMetadataCard({ parsed }: { parsed: ParsedPageDto | null }) {
  if (!parsed) {
    return (
      <Panel title="Page metadata">
        <p className="m-0 text-[13px] text-muted-foreground">No metadata: the page was not parsed.</p>
      </Panel>
    )
  }
  return (
    <Panel title="Page metadata">
      <KeyValues
        rows={[
          ['Title', parsed.title || '—'],
          ['Description', parsed.description || '—'],
          ['Language', parsed.language || '—'],
          ['Canonical', parsed.canonical || '—'],
          ['Robots', parsed.robotsDirectives || '—'],
          ['Links', `${formatNumber(parsed.linkCount)} (${formatNumber(parsed.internalLinkCount)} internal)`],
          ['Images', formatNumber(parsed.imageCount)],
          ['Structured data', structured(parsed)],
        ]}
      />
    </Panel>
  )
}

export function FetchHistoryCard({ page }: { page: PageDetail }) {
  return (
    <Panel title="Fetch history">
      {page.fetches.length === 0 ? (
        <p className="m-0 text-[13px] text-muted-foreground">No fetches recorded.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead scope="col" className={`${TH} px-2`}>Fetched</TableHead>
              <TableHead scope="col" className={`${TH} px-2`}>Status</TableHead>
              <TableHead scope="col" className={`${TH_R} px-2`}>Response</TableHead>
              <TableHead scope="col" className={`${TH_R} px-2`}>Size</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {page.fetches.map((f) => (
              <TableRow key={f.fetchedAt}>
                <TableCell className={`${TD} whitespace-nowrap px-2`}>{formatDate(f.fetchedAt)}</TableCell>
                <TableCell className={`${TD} readout px-2`}>{f.statusCode}</TableCell>
                <TableCell className={`${TD_N} px-2`}>{formatMs(f.responseTimeMs)}</TableCell>
                <TableCell className={`${TD_N} px-2`}>{formatBytes(f.contentLength)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </Panel>
  )
}
