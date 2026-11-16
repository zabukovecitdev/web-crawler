import { useState } from 'react'
import { toast } from 'sonner'
import { EmptyState } from '@/components/shared/States'
import { Button } from '@/components/ui/button'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { MarkdownRender } from './MarkdownRender'
import { usePageMarkdown } from './pages-api'
import { StoredBodyState } from './StoredBodyState'

type Mode = 'rendered' | 'source'

const TRUNCATED_NOTE = 'The stored page is longer than the dashboard shows. This Markdown covers the first part.'

async function copyText(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text)
    toast.success('Markdown copied')
  } catch {
    toast.error('Could not copy the Markdown')
  }
}

function Toolbar({ mode, onMode, text }: { mode: Mode; onMode: (m: Mode) => void; text: string }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-2">
      <Tabs value={mode} onValueChange={(v) => onMode(v as Mode)}>
        <TabsList aria-label="Markdown mode">
          <TabsTrigger value="rendered">Rendered</TabsTrigger>
          <TabsTrigger value="source">Markdown source</TabsTrigger>
        </TabsList>
      </Tabs>
      <Button variant="outline" size="sm" onClick={() => void copyText(text)}>
        Copy
      </Button>
    </div>
  )
}

export function MarkdownView({ id }: { id: number }) {
  const markdown = usePageMarkdown(id, true)
  const [mode, setMode] = useState<Mode>('rendered')
  return (
    <StoredBodyState query={markdown} truncatedNote={TRUNCATED_NOTE}>
      {(data) =>
        data.text.trim() === '' ? (
          <EmptyState title="No readable content" />
        ) : (
          <>
            <Toolbar mode={mode} onMode={setMode} text={data.text} />
            {mode === 'rendered' ? (
              <MarkdownRender markdown={data.text} />
            ) : (
              <pre className="well m-0 max-h-[600px]">{data.text}</pre>
            )}
          </>
        )
      }
    </StoredBodyState>
  )
}
