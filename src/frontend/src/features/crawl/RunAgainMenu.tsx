import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { formatNumber } from '@/lib/format'
import type { CrawlJobDto } from '@/types/api'
import { errorMessage } from './crawl-api'
import { runAgainItems } from './run-again'
import { useJobAction } from './useJobAction'

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

/** One "Run again" menu: retry the failed links, fetch every link again (confirmed), or re-read the stored pages. */
export function RunAgainMenu({ job }: { job: CrawlJobDto }) {
  const [confirming, setConfirming] = useState(false)
  const action = useJobAction(job.id)
  const items = runAgainItems(job)
  if (items.length === 0) {
    return null
  }
  return (
    <div className="flex flex-col items-end gap-2">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" disabled={action.isPending}>
            {action.isPending ? 'Starting…' : 'Run again'}
            <span aria-hidden="true" className="ml-2 text-[10px]">
              ▾
            </span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-72">
          {items.map((item) => (
            <DropdownMenuItem
              key={item.action}
              className="flex-col items-start gap-0.5"
              onSelect={() => (item.action === 'recrawl' ? setConfirming(true) : action.mutate(item.action))}
            >
              <span className="font-medium">{item.label}</span>
              <span className="text-xs text-muted-foreground">{item.description}</span>
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>
      {action.isError && !confirming ? (
        <p role="alert" className="m-0 text-[13px] text-signal-ink">
          <strong>Error:</strong> {errorMessage(action.error)}
        </p>
      ) : null}
      <ConfirmDialog
        job={job}
        open={confirming}
        busy={action.isPending}
        onOpenChange={setConfirming}
        onConfirm={() => action.mutate('recrawl', { onSuccess: () => setConfirming(false) })}
      />
    </div>
  )
}
