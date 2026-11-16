import { useMemo, useRef, useState } from 'react'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { errorMessage } from '@/lib/api'
import { formatNumber } from '@/lib/format'
import type { PageDetail } from '@/types/api'
import { HtmlSourceLines } from './HtmlSourceLines'
import { sourceLines } from './html-highlight'
import { CONTENT_LABELS } from './labels'
import { usePageContent, type PageContent } from './pages-api'
import { isForbidden, isNotFound } from './sites-api'

/** The crawler keeps at most this much of one page (HttpFetcher.MaxBodyBytes). */
const FETCH_CAP_BYTES = 5 * 1024 * 1024

type Check = { tone: 'success' | 'warning'; word: string; detail: string }

/** Compares the stored copy with what the fetch downloaded, so the reader can trust that nothing is missing. */
function checkOf(content: PageContent, downloaded: number | null): Check {
  const stored = content.storedBytes
  const size = stored === null ? '' : `${formatNumber(stored)} bytes stored. `
  if (content.truncated) {
    return { tone: 'warning', word: 'First part only', detail: `${size}The dashboard shows the first 2 MB. Download the file for all of it.` }
  }
  if (downloaded !== null && downloaded >= FETCH_CAP_BYTES) {
    return { tone: 'warning', word: 'Cut at 5 MB', detail: `${size}The crawler keeps at most 5 MB of a page, so the end of this page is missing.` }
  }
  if (stored !== null && downloaded !== null && stored !== downloaded) {
    return { tone: 'warning', word: 'Size differs', detail: `${size}The last fetch downloaded ${formatNumber(downloaded)} bytes.` }
  }
  return { tone: 'success', word: 'Complete', detail: `${size}This is everything the server sent, shown in full below.` }
}

function download(text: string, id: number): void {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/plain;charset=utf-8' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `page-${id}.html.txt`
  anchor.click()
  URL.revokeObjectURL(url)
}

type ViewerProps = { content: PageContent; page: PageDetail }

function Viewer({ content, page }: ViewerProps) {
  const [wrap, setWrap] = useState(true)
  const [tall, setTall] = useState(false)
  const box = useRef<HTMLDivElement>(null)
  const lines = useMemo(() => sourceLines(content.text), [content.text])
  const check = checkOf(content, page.contentLength)
  const toEnd = () => box.current?.scrollTo({ top: box.current.scrollHeight })
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px] leading-[18px]">
        <Badge variant={check.tone}>{check.word}</Badge>
        <span className="text-muted-foreground">
          {formatNumber(lines.length)} lines. {check.detail}
        </span>
      </div>
      <div className="flex flex-wrap gap-2">
        <Button variant="outline" size="sm" aria-pressed={wrap} onClick={() => setWrap(!wrap)}>
          {wrap ? 'Wrapping long lines' : 'Not wrapping lines'}
        </Button>
        <Button variant="outline" size="sm" onClick={() => setTall(!tall)}>
          {tall ? 'Show in a box' : 'Show full height'}
        </Button>
        <Button variant="outline" size="sm" onClick={toEnd}>
          Go to the end
        </Button>
        <Button variant="outline" size="sm" onClick={() => void navigator.clipboard.writeText(content.text)}>
          Copy
        </Button>
        <Button variant="outline" size="sm" onClick={() => download(content.text, page.id)}>
          Download
        </Button>
      </div>
      <div ref={box} tabIndex={0} aria-label="Downloaded HTML source" className={`well ${tall ? '' : 'max-h-[70vh]'} whitespace-normal`}>
        <HtmlSourceLines lines={lines} wrap={wrap} complete={!content.truncated} />
      </div>
    </div>
  )
}

/** The HTML that the crawler downloaded for this page, as text with line numbers and colours. Never run as HTML. */
export function HtmlSource({ page }: { page: PageDetail }) {
  const content = usePageContent(page.id, true)
  if (content.isPending) {
    return <LoadingRows rows={6} />
  }
  if (isNotFound(content.error)) {
    return <EmptyState title="No HTML was stored">{`State: ${CONTENT_LABELS[page.contentState]}. ${page.failReason ?? ''}`}</EmptyState>
  }
  if (content.isError) {
    return isForbidden(content.error) ? (
      <EmptyState title="Stored copy hidden">{errorMessage(content.error)}</EmptyState>
    ) : (
      <ErrorState error={content.error} onRetry={() => void content.refetch()} />
    )
  }
  return <Viewer content={content.data} page={page} />
}
