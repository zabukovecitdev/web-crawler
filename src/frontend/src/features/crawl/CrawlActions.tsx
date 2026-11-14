import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import type { CrawlJobAction, CrawlJobStatus } from '@/types/api'
import { crawlKeys, errorMessage, sendAction } from './crawl-api'

type Props = { jobId: number; host: string; status: CrawlJobStatus }

function useJobAction(jobId: number) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (action: CrawlJobAction) => sendAction(jobId, action),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.job(jobId) })
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
    },
  })
}

type CancelProps = { host: string; open: boolean; busy: boolean; onOpenChange: (open: boolean) => void; onConfirm: () => void }

function CancelDialog({ host, open, busy, onOpenChange, onConfirm }: CancelProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader className="gap-2 text-left">
          <DialogTitle>Cancel this crawl?</DialogTitle>
          <DialogDescription>
            SamoBot stops asking {host} for pages. What was already crawled stays. A cancelled crawl cannot be resumed.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            Keep crawling
          </Button>
          <Button onClick={onConfirm} disabled={busy}>
            {busy ? 'Cancelling…' : 'Cancel crawl'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** Pause or resume, and cancel behind a confirmation dialog. */
export function CrawlActions({ jobId, host, status }: Props) {
  const [confirming, setConfirming] = useState(false)
  const action = useJobAction(jobId)
  const paused = status === 'Paused'
  const run = (next: CrawlJobAction) => action.mutate(next, { onSuccess: () => setConfirming(false) })

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <Button variant="outline" disabled={action.isPending} onClick={() => run(paused ? 'start' : 'pause')}>
          {paused ? 'Resume' : 'Pause'}
        </Button>
        <Button variant="outline" disabled={action.isPending} onClick={() => setConfirming(true)}>
          Cancel crawl
        </Button>
      </div>
      {action.isError && !confirming ? (
        <p role="alert" className="m-0 w-full text-[13px] text-signal-ink">
          <strong>Error:</strong> {errorMessage(action.error)}
        </p>
      ) : null}
      <CancelDialog host={host} open={confirming} busy={action.isPending} onOpenChange={setConfirming} onConfirm={() => run('cancel')} />
    </>
  )
}
