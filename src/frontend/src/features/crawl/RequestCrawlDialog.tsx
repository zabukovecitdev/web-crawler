import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { RequestCrawlForm } from './RequestCrawlForm'

export type RequestCrawlDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  initialUrl?: string
}

/** "Request a crawl": one address, optional limits. Starts the job and opens its live screen. */
export function RequestCrawlDialog({ open, onOpenChange, initialUrl }: RequestCrawlDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[92vh] max-w-xl overflow-y-auto">
        <DialogHeader className="gap-2 text-left">
          <p className="label">New crawl</p>
          <DialogTitle className="text-[22px] leading-7 tracking-[-0.01em]">Request a crawl</DialogTitle>
          <DialogDescription>
            Give the address of the site. The crawl starts at this page and stays on this host.
          </DialogDescription>
        </DialogHeader>
        <RequestCrawlForm initialUrl={initialUrl} onDone={() => onOpenChange(false)} />
      </DialogContent>
    </Dialog>
  )
}
