import { useState } from 'react'
import { Link } from 'react-router-dom'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { ToneBadge } from '@/components/shared/StatusBadge'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatNumber } from '@/lib/format'
import type { SitemapDto, SitemapFileDto, SitemapUrlDto } from '@/types/api'
import { Pager } from './Pager'
import { Panel } from './Panel'
import { useSitemap } from './sitemap-api'
import { isNotFound } from './sites-api'
import { LINK, TD, TH } from './table-styles'

const LIMIT = 25
const dateOnly = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
export const NO_SITEMAP = 'No sitemap found yet. It is read after a crawl finishes.'

function Coverage({ sitemap }: { sitemap: SitemapDto }) {
  const { urlCount, crawledCount } = sitemap
  const ratio = urlCount === 0 ? 0 : crawledCount / urlCount
  const percent = Math.round(ratio * 100)
  const text = `${formatNumber(crawledCount)} of ${formatNumber(urlCount)}`
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap justify-between gap-2 text-[13px]">
        <span>Sitemap URLs that were crawled</span>
        <span className="readout">{text} · {percent} %</span>
      </div>
      <div
        role="img"
        aria-label={`${text} sitemap URLs crawled, ${percent} percent`}
        className="h-2.5 rounded bg-muted shadow-[var(--shadow-inset)]"
      >
        <div className="h-full rounded bg-muted-foreground" style={{ width: `${ratio * 100}%` }} />
      </div>
      <p className="m-0 text-[13px] text-muted-foreground">
        {formatNumber(urlCount - crawledCount)} URLs are in the sitemap but were not crawled.{' '}
        {formatNumber(sitemap.notInSitemapCount)} pages were crawled but are not in the sitemap.
      </p>
    </div>
  )
}

function FileNode({ file, files }: { file: SitemapFileDto; files: SitemapFileDto[] }) {
  const children = files.filter((f) => f.parentUrl === file.url)
  const meta = file.kind === 'Index' ? `Index · ${children.length} sitemaps` : `${formatNumber(file.urlCount)} URLs`
  return (
    <li>
      <div className="flex flex-wrap items-center justify-between gap-3 py-1.5">
        <span>{file.url}</span>
        <span className="text-muted-foreground">{meta}</span>
      </div>
      {children.length > 0 ? (
        <ul className="m-0 ml-[7px] list-none border-l border-line-strong p-0 pl-4">
          {children.map((c) => (
            <FileNode key={c.url} file={c} files={files} />
          ))}
        </ul>
      ) : null}
    </li>
  )
}

function FileTree({ files }: { files: SitemapFileDto[] }) {
  const urls = new Set(files.map((f) => f.url))
  const roots = files.filter((f) => f.parentUrl === null || !urls.has(f.parentUrl))
  return (
    <ul aria-label="Sitemap files" className="m-0 list-none p-0 font-mono text-[13px] font-medium leading-[18px]">
      {roots.map((f) => (
        <FileNode key={f.url} file={f} files={files} />
      ))}
    </ul>
  )
}

const CRAWL: Record<SitemapUrlDto['crawlState'], { word: string; tone: 'neutral' | 'warning' | 'success' }> = {
  Crawled: { word: 'Crawled', tone: 'success' },
  NotCrawled: { word: 'Not crawled', tone: 'neutral' },
  Blocked: { word: 'Blocked', tone: 'warning' },
}

function UrlRow({ host, row }: { host: string; row: SitemapUrlDto }) {
  const look = CRAWL[row.crawlState]
  return (
    <TableRow>
      <TableCell className={`${TD} readout`}>
        {row.pageId ? (
          <Link className={LINK} to={`/sites/${encodeURIComponent(host)}/pages/${row.pageId}`}>
            {row.loc}
          </Link>
        ) : (
          row.loc
        )}
      </TableCell>
      <TableCell className={`${TD} whitespace-nowrap text-muted-foreground`}>{row.lastModified ? dateOnly.format(new Date(row.lastModified)) : '—'}</TableCell>
      <TableCell className={TD}>
        <ToneBadge tone={look.tone}>{row.statusCode ? `${look.word}, ${row.statusCode}` : look.word}</ToneBadge>
      </TableCell>
    </TableRow>
  )
}

function UrlTable({ host, sitemap, offset, onPage }: { host: string; sitemap: SitemapDto; offset: number; onPage: (o: number) => void }) {
  const { items, total } = sitemap.urls
  return (
    <>
      <Table className="min-w-[480px]">
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead scope="col" className={TH}>URL in sitemap</TableHead>
            <TableHead scope="col" className={TH}>Last modified</TableHead>
            <TableHead scope="col" className={TH}>Crawl</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((row) => (
            <UrlRow key={row.loc} host={host} row={row} />
          ))}
        </TableBody>
      </Table>
      <Pager total={total} offset={offset} limit={LIMIT} shown={items.length} onChange={onPage} />
    </>
  )
}

export function SitemapCard({ host }: { host: string }) {
  const [offset, setOffset] = useState(0)
  const sitemap = useSitemap(host, LIMIT, offset)
  if (sitemap.isPending) {
    return <Panel title="Sitemap"><LoadingRows rows={6} /></Panel>
  }
  if (sitemap.isError && !isNotFound(sitemap.error)) {
    return <Panel title="Sitemap"><ErrorState error={sitemap.error} onRetry={() => void sitemap.refetch()} /></Panel>
  }
  const data = sitemap.data
  if (!data || data.files.length === 0) {
    return <Panel title="Sitemap"><EmptyState title="No sitemap">{NO_SITEMAP}</EmptyState></Panel>
  }
  return (
    <Panel title="Sitemap" kicker={`${data.files.length} files · ${formatNumber(data.urlCount)} URLs`}>
      <Coverage sitemap={data} />
      <FileTree files={data.files} />
      <UrlTable host={host} sitemap={data} offset={offset} onPage={setOffset} />
    </Panel>
  )
}
