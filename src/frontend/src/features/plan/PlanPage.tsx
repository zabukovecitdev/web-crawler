import { useQuery } from '@tanstack/react-query'
import { Link, Navigate, useNavigate, useSearchParams } from 'react-router-dom'
import { ErrorState, LoadingRows } from '@/components/shared/States'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Panel } from '../sites/Panel'
import { PlanActionBar } from './PlanActionBar'
import { PlanMapCard } from './PlanMapCard'
import { PlanNotices } from './PlanNotices'
import { PlanOptions } from './PlanOptions'
import { SiteCheckList } from './SiteCheckList'
import { checkRows } from './check-rows'
import { BROWSER_UNAVAILABLE, checkSite, errorCode, errorMessage, planKeys } from './plan-api'
import { secondsPerPage, summarize, usesBrowser } from './plan-summary'
import { blockerOf } from './render-hint'
import { useBrowserState, usePlanChoices, useSitemapPlan, useStartCrawl } from './usePlanHooks'
import { usePlanMap } from './usePlanMap'
import type { SiteCheckDto } from '@/types/api'

function PlanHeader({ check }: { check: SiteCheckDto }) {
  const navigate = useNavigate()
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div className="flex min-w-0 flex-col gap-2">
        <p className="label">
          <Link to="/crawls" className="hover:underline">
            Crawls
          </Link>{' '}
          / New crawl
        </p>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="m-0 break-all text-[32px] font-semibold leading-[38px] tracking-[-0.02em]">{check.host}</h1>
          <Badge variant="neutral">Not started</Badge>
        </div>
        <p className="m-0 text-[13px] text-muted-foreground">
          Start page <span className="readout">{new URL(check.startUrl).pathname}</span> · you entered <span className="readout [overflow-wrap:anywhere]">{check.requestedUrl}</span>
        </p>
      </div>
      <Button variant="outline" onClick={() => navigate('/crawls')}>
        Back
      </Button>
    </div>
  )
}

function whyBlocked(check: SiteCheckDto, gated: boolean): string | null {
  if (!check.startPage.reachable) {
    return 'The start page can’t be fetched, so there is nothing to crawl.'
  }
  return gated ? 'Start the browser service or choose another way to fetch pages.' : null
}

function PlanBody({ check }: { check: SiteCheckDto }) {
  const choices = usePlanChoices()
  const browser = useBrowserState(check.browser)
  const { plan, waitedTooLong } = useSitemapPlan(check)
  const hasSitemap = plan.state === 'Ready'
  const map = usePlanMap(check, hasSitemap, choices)
  const start = useStartCrawl(check, { rules: choices.rules, mode: choices.mode, linked: choices.linked, review: choices.review, hasSitemap }, browser.reportDown)
  const verdict = check.javaScript.verdict
  const inBrowser = usesBrowser(choices.mode, verdict)
  const gated = blockerOf(choices.mode, verdict, browser.status.available) !== null
  const why = whyBlocked(check, gated)
  const seconds = map.ticked * secondsPerPage(check.robots.crawlDelaySeconds, inBrowser)
  const summary = summarize({ ticked: map.ticked, total: map.total, hasSitemap, seconds, inBrowser, linkedExtra: choices.linked, review: choices.review })
  return (
    <div className="flex flex-col gap-6">
      <PlanHeader check={check} />
      <PlanNotices check={check} mode={choices.mode} browser={browser} sitemapWaitedTooLong={waitedTooLong} onMode={choices.setMode} />
      <Panel title="What SamoBot found" kicker={`Site check · ${new Date(check.checkedAt).toTimeString().slice(0, 8)}`}>
        <SiteCheckList columns rows={checkRows({ ...check, sitemap: plan, browser: browser.status })} />
      </Panel>
      <PlanMapCard map={map} choices={choices} hasSitemap={hasSitemap} sitemapPending={plan.state === 'Pending' && !waitedTooLong} />
      <PlanOptions
        hasSitemap={hasSitemap}
        linked={choices.linked}
        review={choices.review}
        mode={choices.mode}
        check={check}
        browserUp={browser.status.available}
        onLinked={choices.setLinked}
        onReview={choices.setReview}
        onMode={choices.setMode}
      />
      <PlanActionBar summary={summary} disabled={why !== null} why={why} pending={start.isPending} error={start.isError && errorCode(start.error) !== BROWSER_UNAVAILABLE ? errorMessage(start.error) : null} onStart={() => start.mutate()} />
    </div>
  )
}

export function PlanPage() {
  const [params] = useSearchParams()
  const url = params.get('url')
  const check = useQuery({
    queryKey: planKeys.check(url ?? ''),
    queryFn: () => checkSite(url ?? ''),
    enabled: url !== null,
    staleTime: 5 * 60_000,
    retry: false,
    refetchOnWindowFocus: false,
  })
  if (url === null) {
    return <Navigate to="/crawls" replace />
  }
  if (check.isError) {
    return <ErrorState error={check.error} onRetry={() => void check.refetch()} />
  }
  return check.data ? <PlanBody key={check.data.checkedAt} check={check.data} /> : <LoadingRows rows={8} />
}
