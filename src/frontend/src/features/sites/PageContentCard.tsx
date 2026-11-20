import { useSearchParams } from 'react-router-dom'
import { EmptyState } from '@/components/shared/States'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatNumber } from '@/lib/format'
import type { PageDetail, ParsedPageDto } from '@/types/api'
import { HtmlSource } from './HtmlSource'
import { CONTENT_VIEWS, DEFAULT_CONTENT_VIEW, contentViewOf, type ContentView } from './content-views'
import { CONTENT_LABELS } from './labels'
import { MarkdownView } from './MarkdownView'
import { Panel } from './Panel'

function TextView({ parsed }: { parsed: ParsedPageDto }) {
  const paragraphs = parsed.bodyText.split(/\n{2,}/).filter((p) => p.trim() !== '')
  return (
    <div className="flex max-w-[68ch] flex-col gap-3">
      {paragraphs.map((text, i) => (
        <p key={i} className="m-0">
          {text}
        </p>
      ))}
    </div>
  )
}

function ViewBody({ view, page }: { view: ContentView; page: PageDetail }) {
  if (view === 'rendered' || view === 'markdown') {
    return <MarkdownView id={page.id} mode={view} />
  }
  if (view === 'html') {
    return <HtmlSource page={page} />
  }
  if (!page.parsed) {
    return (
      <EmptyState title="No parsed content">
        {`State: ${CONTENT_LABELS[page.contentState]}. ${page.failReason ?? ''}`}
      </EmptyState>
    )
  }
  return <TextView parsed={page.parsed} />
}

export function PageContentCard({ page }: { page: PageDetail }) {
  const [params, setParams] = useSearchParams()
  const view = contentViewOf(params.get('view'))
  const parsed = page.parsed
  const kicker = parsed
    ? `${formatNumber(parsed.wordCount)} words · ${formatNumber(parsed.bodyText.length)} characters`
    : undefined
  const select = (next: string) =>
    setParams(
      (current) => {
        const copy = new URLSearchParams(current)
        if (next === DEFAULT_CONTENT_VIEW) {
          copy.delete('view')
        } else {
          copy.set('view', next)
        }
        return copy
      },
      { replace: true },
    )
  return (
    <Panel title="Content" kicker={kicker}>
      <Tabs value={view} onValueChange={select}>
        <TabsList aria-label="Show the content as" className="h-auto flex-wrap">
          {CONTENT_VIEWS.map((v) => (
            <TabsTrigger key={v.value} value={v.value}>
              {v.label}
            </TabsTrigger>
          ))}
        </TabsList>
        {CONTENT_VIEWS.map((v) => (
          <TabsContent key={v.value} value={v.value}>
            <ViewBody view={v.value} page={page} />
          </TabsContent>
        ))}
      </Tabs>
    </Panel>
  )
}
