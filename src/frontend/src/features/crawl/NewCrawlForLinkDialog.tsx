import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import type { CrawlJobDto, JobLinkDto } from '@/types/api'
import { createJob, crawlKeys, errorMessage } from './crawl-api'

function useCrawlOneLink(job: CrawlJobDto) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (url: string) =>
      createJob({ seedUrls: [url], maxDepth: 0, maxUrls: 1, useJsRendering: job.useJsRendering, startImmediately: true }),
    onSuccess: (created) => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
      navigate(`/crawls/${created.id}`)
    },
  })
}

type Props = { job: CrawlJobDto; link: JobLinkDto | null; onClose: () => void }

export function NewCrawlForLinkDialog({ job, link, onClose }: Props) {
  const create = useCrawlOneLink(job)
  return (
    <Dialog open={link !== null} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent className="max-w-md">
        <DialogHeader className="gap-2 text-left">
          <DialogTitle>Start a new crawl for this link?</DialogTitle>
          <DialogDescription>
            Crawl #{job.id} was cancelled, so its links are not fetched again. SamoBot can start a new crawl that fetches only this
            link. The cancelled crawl stays closed.
          </DialogDescription>
        </DialogHeader>
        <p className="readout m-0 text-[13px] [overflow-wrap:anywhere]">{link?.url}</p>
        {create.isError ? (
          <p role="alert" className="m-0 text-[13px] text-signal-ink">
            <strong>The crawl was not started.</strong> {errorMessage(create.error)}
          </p>
        ) : null}
        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="ghost" onClick={onClose}>
            Not now
          </Button>
          <Button disabled={create.isPending || link === null} onClick={() => link && create.mutate(link.url)}>
            {create.isPending ? 'Starting…' : 'Start new crawl'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
