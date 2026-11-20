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

type Check = { tone: 'success' | 'warning'; word: string; detail: string }

function checkOf(content: PageContent, page: PageDetail): Check {
  const downloaded = page.contentLength
  const stored = content.storedBytes
  const size = stored === null ? '' : `${formatNumber(stored)} bytes stored. `
  if (content.truncated) {
    return { tone: 'warning', word: 'First part only', detail: `${size}The dashboard shows the first 2 MB. Download the file for all of it.` }
  }
  if (page.bodyTruncated) {
    return { tone: 'warning', word: 'Cut at the size cap', detail: `${size}The crawler stopped reading this page at its size cap, so the end of the page is missing.` }
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
  const check = checkOf(content, page)
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
