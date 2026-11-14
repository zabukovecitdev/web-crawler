import { useState, type ReactNode } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { Button } from '@/components/ui/button'
import { RequestCrawlDialog } from '@/features/crawl/RequestCrawlDialog'
import { formatDate, formatMs, formatNumber } from '@/lib/format'
import type { SiteDetail } from '@/types/api'
import { SectionTabs } from './SectionTabs'
import { SiteStatusBadge } from './SiteStatusBadge'
import { isNotFound, useSite } from './sites-api'

function Figure({ label, value }: { label: string; value: string }) {
  return (
    <p className="label">
      {label} · <span className="text-foreground">{value}</span>
    </p>
  )
}

function Figures({ site }: { site: SiteDetail }) {
  return (
    <div className="flex flex-wrap gap-x-8 gap-y-3">
      <Figure label="Pages" value={formatNumber(site.pages)} />
      <Figure label="Failed" value={formatNumber(site.failed)} />
      <Figure label="Median response" value={formatMs(site.avgResponseMs)} />
      <Figure label="Delay" value={formatMs(site.crawlDelayMs)} />
      <Figure label="First crawl" value={formatDate(site.firstCrawlAt)} />
    </div>
  )
}

function SiteTabsNav({ host }: { host: string }) {
  const { pathname, search } = useLocation()
  const base = `/sites/${encodeURIComponent(host)}`
  const robots = new URLSearchParams(search).get('tab') === 'robots'
  const onSitemap = pathname === `${base}/sitemap`
  return (
    <SectionTabs
      label="Site sections"
      tabs={[
        { to: base, label: 'Pages', active: !onSitemap },
        { to: `${base}/sitemap`, label: 'Sitemap', active: onSitemap && !robots },
        { to: `${base}/sitemap?tab=robots`, label: 'Robots.txt', active: onSitemap && robots },
      ]}
    />
  )
}

function SiteHeading({ site }: { site: SiteDetail }) {
  const [dialog, setDialog] = useState(false)
  return (
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div className="flex min-w-0 flex-col gap-2">
        <p className="label">
          <Link to="/" className="text-inherit no-underline hover:underline">
            Sites
          </Link>{' '}
          / Site
        </p>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="m-0 break-all text-[32px] font-semibold leading-[38px] tracking-[-0.02em]">{site.host}</h1>
          <SiteStatusBadge status={site.status} />
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-3">
        {site.activeJobId ? (
          <Link to={`/crawls/${site.activeJobId}`} className="text-[13px] font-medium text-foreground hover:underline">
            Open live crawl
          </Link>
        ) : null}
        <Button variant="outline" onClick={() => setDialog(true)}>
          Crawl again
        </Button>
      </div>
      <RequestCrawlDialog open={dialog} onOpenChange={setDialog} initialUrl={`https://${site.host}`} />
    </div>
  )
}

type Props = { host: string; children: ReactNode }

/** Header, key figures and section tabs shared by the site's pages and sitemap screens. */
export function SiteFrame({ host, children }: Props) {
  const site = useSite(host)
  if (site.isPending) {
    return <LoadingRows rows={4} />
  }
  if (site.isError) {
    return isNotFound(site.error) ? (
      <EmptyState title="Site not found">
        No crawl has seen {host} yet. <Link to="/">Back to sites</Link>
      </EmptyState>
    ) : (
      <ErrorState error={site.error} onRetry={() => void site.refetch()} />
    )
  }
  return (
    <div className="flex flex-col gap-6">
      <SiteHeading site={site.data} />
      <Figures site={site.data} />
      <SiteTabsNav host={host} />
      {children}
    </div>
  )
}
