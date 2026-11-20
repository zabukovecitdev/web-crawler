import { toast } from 'sonner'
import { EmptyState } from '@/components/shared/States'
import { Button } from '@/components/ui/button'
import { MarkdownRender } from './MarkdownRender'
import { usePageMarkdown } from './pages-api'
import { StoredBodyState } from './StoredBodyState'

const TRUNCATED_NOTE = 'The stored page is longer than the dashboard shows. This Markdown covers the first part.'

async function copyText(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text)
    toast.success('Markdown copied')
  } catch {
    toast.error('Could not copy the Markdown')
  }
}

type Props = { id: number; mode: 'rendered' | 'markdown' }

export function MarkdownView({ id, mode }: Props) {
  const markdown = usePageMarkdown(id, true)
  return (
    <StoredBodyState query={markdown} truncatedNote={TRUNCATED_NOTE}>
      {(data) =>
        data.text.trim() === '' ? (
          <EmptyState title="No readable content" />
        ) : (
          <div className="flex flex-col gap-3">
            <div className="flex justify-end">
              <Button variant="outline" size="sm" onClick={() => void copyText(data.text)}>
                Copy Markdown
              </Button>
            </div>
            {mode === 'rendered' ? (
              <MarkdownRender markdown={data.text} />
            ) : (
              <pre className="well m-0 max-h-[600px]">{data.text}</pre>
            )}
          </div>
        )
      }
    </StoredBodyState>
  )
}
