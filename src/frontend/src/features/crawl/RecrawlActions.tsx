import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { formatNumber } from '@/lib/format'
import type { CrawlJobAction, CrawlJobDto } from '@/types/api'
import { crawlKeys, errorMessage, sendAction } from './crawl-api'

/** Sends a recrawl action and refreshes everything that shows the job. */
function useRecrawl(jobId: number) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (action: CrawlJobAction) => sendAction(jobId, action),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.job(jobId) })
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
    },
  })
}

type ConfirmProps = { job: CrawlJobDto; open: boolean; busy: boolean; onOpenChange: (open: boolean) => void; onConfirm: () => void }

function ConfirmDialog({ job, open, busy, onOpenChange, onConfirm }: ConfirmProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader className="gap-2 text-left">
          <DialogTitle>Recrawl the whole job?</DialogTitle>
          <DialogDescription>
            SamoBot asks {job.host} for all {formatNumber(job.links.total)} links again, at the polite pace of the site. Pages stay
            searchable while they are fetched again.
            {job.status === 'Paused' ? ' The crawl is paused: resume it to start.' : ''}
          </DialogDescription>
        </DialogHeader>
        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            Keep as it is
          </Button>
          <Button onClick={onConfirm} disabled={busy}>
            {busy ? 'Queueing…' : 'Recrawl all links'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** Retry the failed links, or fetch every link of the job again (behind a confirmation). */
export function RecrawlActions({ job }: { job: CrawlJobDto }) {
  const [confirming, setConfirming] = useState(false)
  const recrawl = useRecrawl(job.id)
  const failed = job.links.failed + job.links.retrying
  if (job.status === 'Draft' || job.status === 'Pending' || job.links.total === 0) {
    return null
  }
  return (
    <div className="flex flex-col items-end gap-2">
      <div className="flex flex-wrap items-center gap-3">
        {failed > 0 ? (
          <Button disabled={recrawl.isPending} onClick={() => recrawl.mutate('retry-failed')}>
            Retry {formatNumber(failed)} failed
          </Button>
        ) : null}
        <Button variant="outline" disabled={recrawl.isPending} onClick={() => setConfirming(true)}>
          Recrawl all
        </Button>
      </div>
      {recrawl.isError && !confirming ? (
        <p role="alert" className="m-0 text-[13px] text-signal-ink">
          <strong>Error:</strong> {errorMessage(recrawl.error)}
        </p>
      ) : null}
      <ConfirmDialog
        job={job}
        open={confirming}
        busy={recrawl.isPending}
        onOpenChange={setConfirming}
        onConfirm={() => recrawl.mutate('recrawl', { onSuccess: () => setConfirming(false) })}
      />
    </div>
  )
}
