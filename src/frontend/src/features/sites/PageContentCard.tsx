import { useState } from 'react'
import { EmptyState } from '@/components/shared/States'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatNumber } from '@/lib/format'
import type { PageDetail, ParsedPageDto } from '@/types/api'
import { HtmlSource } from './HtmlSource'
import { CONTENT_LABELS } from './labels'
import { PageLinksView } from './PageLinksView'
import { Panel } from './Panel'

type View = 'text' | 'html' | 'headings' | 'links'
const VIEWS: View[] = ['text', 'html', 'headings', 'links']

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

function HeadingsView({ parsed }: { parsed: ParsedPageDto }) {
  if (parsed.headings.length === 0) {
    return <p className="m-0 text-muted-foreground">This page has no headings.</p>
  }
  return (
    <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
      {parsed.headings.map((h, i) => (
        <li key={i} style={{ paddingLeft: `${(h.level - 1) * 16}px` }} className="flex items-baseline gap-3">
          <span className="label w-6 shrink-0">H{h.level}</span>
          <span>{h.text}</span>
        </li>
      ))}
    </ul>
  )
}

function ViewBody({ view, page }: { view: View; page: PageDetail }) {
  if (view === 'html') {
    return <HtmlSource page={page} />
  }
  if (view === 'links') {
    return <PageLinksView id={page.id} />
  }
  if (!page.parsed) {
    return (
      <EmptyState title="No parsed content">
        {`State: ${CONTENT_LABELS[page.contentState]}. ${page.failReason ?? ''}`}
      </EmptyState>
    )
  }
  if (view === 'headings') {
    return <HeadingsView parsed={page.parsed} />
  }
  return <TextView parsed={page.parsed} />
}

export function PageContentCard({ page }: { page: PageDetail }) {
  const [view, setView] = useState<View>(page.parsed ? 'text' : 'html')
  const parsed = page.parsed
  const kicker = parsed
    ? `${formatNumber(parsed.wordCount)} words · ${formatNumber(parsed.bodyText.length)} characters`
    : undefined
  return (
    <Panel title="Content" kicker={kicker}>
      <Tabs value={view} onValueChange={(v) => setView(v as View)}>
        <TabsList aria-label="Content view" className="h-auto flex-wrap">
          <TabsTrigger value="text">Text</TabsTrigger>
          <TabsTrigger value="html">Downloaded HTML</TabsTrigger>
          <TabsTrigger value="headings">Headings {parsed ? parsed.headings.length : 0}</TabsTrigger>
          <TabsTrigger value="links">Links {parsed ? formatNumber(parsed.linkCount) : 0}</TabsTrigger>
        </TabsList>
        {VIEWS.map((v) => (
          <TabsContent key={v} value={v}>
            <ViewBody view={v} page={page} />
          </TabsContent>
        ))}
      </Tabs>
    </Panel>
  )
}
