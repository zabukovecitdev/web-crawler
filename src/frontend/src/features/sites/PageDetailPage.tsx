import { Link, useParams } from 'react-router-dom'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { StatusBadge } from '@/components/shared/StatusBadge'
import { Button } from '@/components/ui/button'
import type { PageDetail } from '@/types/api'
import { PageContentCard } from './PageContentCard'
import { CrawlMetadataCard, FetchHistoryCard, PageMetadataCard } from './PageMetaCards'
import { usePage } from './pages-api'
import { isNotFound } from './sites-api'

function Heading({ page, host }: { page: PageDetail; host: string }) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div className="flex min-w-0 flex-col gap-2">
        <p className="label">
          <Link to="/" className="text-inherit no-underline hover:underline">Sites</Link> /{' '}
          <Link to={`/sites/${encodeURIComponent(host)}`} className="text-inherit no-underline hover:underline">
            {host}
          </Link>{' '}
          / Page
        </p>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="m-0 break-words text-[32px] font-semibold leading-[38px] tracking-[-0.02em]">
            {page.title || page.path}
          </h1>
          <StatusBadge code={page.statusCode} />
        </div>
        <p className="readout m-0 text-muted-foreground [overflow-wrap:anywhere]">{page.url}</p>
      </div>
      <Button asChild variant="outline">
        <a href={page.url} target="_blank" rel="noopener noreferrer">
          Open original page
        </a>
      </Button>
    </div>
  )
}

export function PageDetailPage() {
  const params = useParams()
  const host = params.host ?? ''
  const page = usePage(Number(params.id))
  if (page.isPending) {
    return <LoadingRows rows={6} />
  }
  if (page.isError) {
    return isNotFound(page.error) ? (
      <EmptyState title="Page not found">
        This page is not in the crawl results. <Link to={`/sites/${encodeURIComponent(host)}`}>Back to the site</Link>
      </EmptyState>
    ) : (
      <ErrorState error={page.error} onRetry={() => void page.refetch()} />
    )
  }
  return (
    <div className="flex flex-col gap-6">
      <Heading page={page.data} host={host} />
      <div className="grid grid-cols-1 items-start gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <PageContentCard page={page.data} />
        <div className="flex min-w-0 flex-col gap-6">
          <CrawlMetadataCard page={page.data} />
          <PageMetadataCard parsed={page.data.parsed} />
          <FetchHistoryCard page={page.data} />
        </div>
      </div>
    </div>
  )
}
