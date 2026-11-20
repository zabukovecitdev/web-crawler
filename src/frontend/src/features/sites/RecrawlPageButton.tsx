import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { apiFetch, errorMessage } from '@/lib/api'
import { crawlJobsKey } from '@/lib/crawl-jobs'
import type { PageDetail } from '@/types/api'

function useRecrawlPage(page: PageDetail) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: () => apiFetch<void>(`/api/v1/crawl-jobs/${page.crawlJobId}/links/${page.id}/recrawl`, { method: 'POST' }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['page', page.id] })
      void queryClient.invalidateQueries({ queryKey: crawlJobsKey })
    },
  })
}

export function RecrawlPageButton({ page }: { page: PageDetail }) {
  const recrawl = useRecrawlPage(page)
  if (page.crawlJobId === null) {
    return null
  }
  return (
    <div className="flex flex-col items-start gap-1">
      <Button variant="outline" disabled={recrawl.isPending || recrawl.isSuccess} onClick={() => recrawl.mutate()}>
        {recrawl.isSuccess ? 'Queued for recrawl' : 'Recrawl this page'}
      </Button>
      {recrawl.isSuccess ? (
        <Link className="text-[13px]" to={`/crawls/${page.crawlJobId}`}>
          Follow it in crawl #{page.crawlJobId}
        </Link>
      ) : null}
      {recrawl.isError ? (
        <p role="alert" className="m-0 max-w-xs text-[13px] text-signal-ink">
          {errorMessage(recrawl.error)}
        </p>
      ) : null}
    </div>
  )
}
