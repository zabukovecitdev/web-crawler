// Stub owned by the crawl feature: it is replaced by the real dialog. Other features import it
// from this path with these props, so keep the signature.
export type RequestCrawlDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  initialUrl?: string
}

export function RequestCrawlDialog({ open }: RequestCrawlDialogProps) {
  return open ? <div role="dialog" aria-label="Request a crawl" /> : null
}
