import { Notice } from '@/components/shared/Notice'
import { Button } from '@/components/ui/button'
import type { RenderMode, SiteCheckDto } from '@/types/api'
import { noticeKinds } from './plan-notices'
import type { BrowserState } from './usePlanHooks'

type Props = {
  check: SiteCheckDto
  mode: RenderMode
  browser: BrowserState
  sitemapWaitedTooLong: boolean
  onMode: (mode: RenderMode) => void
}

function GateNotice({ check, mode, browser, onMode }: Omit<Props, 'sitemapWaitedTooLong'>) {
  const always = mode === 'Always'
  const because = always
    ? 'You chose to render every page in a browser.'
    : `This site needs JavaScript: without it the start page has ${check.javaScript.plain.textLength} characters of text and ${check.javaScript.plain.linkCount} links.`
  const altLabel = always && check.javaScript.verdict === 'NotNeeded' ? 'Use auto-detect' : 'Crawl without JavaScript'
  return (
    <Notice
      tone="error"
      title="The crawl can’t start: the browser service is not available."
      actions={
        <>
          <Button variant="outline" size="sm" disabled={browser.checking} onClick={browser.recheck}>
            {browser.checking ? 'Checking…' : 'Check again'}
          </Button>
          <Button variant="outline" size="sm" onClick={() => onMode(altLabel === 'Use auto-detect' ? 'Auto' : 'Never')}>
            {altLabel}
          </Button>
        </>
      }
    >
      <p className="m-0">{because}</p>
      <p className="m-0 text-[13px] text-muted-foreground">
        Start the browser service, then check again. On the server: <code>CHROME_RENDERING_ENABLED=true docker compose --profile js up</code>
      </p>
      {browser.checkedNote ? <p className="m-0 text-[13px]">{browser.checkedNote}</p> : null}
    </Notice>
  )
}

export function PlanNotices(props: Props) {
  const { check, mode, browser } = props
  const kinds = noticeKinds({ mode, verdict: check.javaScript.verdict, browserUp: browser.status.available, sitemapWaitedTooLong: props.sitemapWaitedTooLong })
  const { plain } = check.javaScript
  return (
    <>
      {kinds.gate ? <GateNotice check={check} mode={mode} browser={browser} onMode={props.onMode} /> : null}
      {browser.checkedNote && !kinds.gate ? (
        <Notice tone="info" title="The browser service answers.">
          <p className="m-0">{browser.checkedNote}</p>
        </Notice>
      ) : null}
      {kinds.never ? (
        <Notice tone="warn" title="Pages will be stored almost empty.">
          <p className="m-0">
            Without JavaScript the start page has {plain.textLength} characters of text and {plain.linkCount} {plain.linkCount === 1 ? 'link' : 'links'}.
            Search and Markdown will have little to work with. You can start anyway.
          </p>
        </Notice>
      ) : null}
      {kinds.browserInfo ? (
        <Notice tone="info" title="The browser service is not available. This site does not need it.">
          <p className="m-0">
            The start page has its content without JavaScript. If a later page needs JavaScript, that page waits for the browser and the rest of the crawl goes on.
          </p>
        </Notice>
      ) : null}
      {kinds.sitemapSlow ? (
        <Notice tone="info" title="The sitemap is still being read.">
          <p className="m-0">It takes longer than usual. You can start now: the crawl then follows links from the start page.</p>
        </Notice>
      ) : null}
    </>
  )
}
