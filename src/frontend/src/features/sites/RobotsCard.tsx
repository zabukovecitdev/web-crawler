import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { formatDate, formatMs } from '@/lib/format'
import { ExplainForm } from './ExplainForm'
import { KeyValues } from './KeyValues'
import { Panel } from './Panel'
import { robotsFacts } from './robots-facts'
import { useRobots } from './sitemap-api'
import { isNotFound } from './sites-api'

export function RobotsCard({ host }: { host: string }) {
  const robots = useRobots(host)
  if (robots.isPending) {
    return <Panel title="Robots.txt"><LoadingRows rows={5} /></Panel>
  }
  if (robots.isError) {
    return isNotFound(robots.error) ? (
      <Panel title="Robots.txt">
        <EmptyState title="No robots.txt">No robots.txt has been read for this site yet. It is fetched when a crawl starts.</EmptyState>
      </Panel>
    ) : (
      <Panel title="Robots.txt"><ErrorState error={robots.error} onRetry={() => void robots.refetch()} /></Panel>
    )
  }
  const data = robots.data
  const facts = robotsFacts(data)
  return (
    <Panel title="Robots.txt" kicker={`Fetched ${formatDate(data.fetchedAt)} · expires ${formatDate(data.expiresAt)}`}>
      <KeyValues
        rows={[
          ['Our user agent', 'SamoBot'],
          ['Crawl delay', formatMs(data.crawlDelayMs)],
          ['Blocked paths', String(facts.blockedPaths)],
          ['Sitemaps named', String(facts.sitemapsNamed)],
        ]}
      />
      <pre className="well m-0 max-h-[360px]">{data.content}</pre>
      <hr className="m-0 h-px border-0 bg-border" />
      <ExplainForm host={host} />
    </Panel>
  )
}
