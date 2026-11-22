import { useState } from 'react'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { CheckSiteForm, type DialogPhase } from '../plan/CheckSiteForm'

export type RequestCrawlDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  initialUrl?: string
}

const COPY: Record<DialogPhase, { title: string; text: string }> = {
  start: {
    title: 'Request a crawl',
    text: 'Enter the page to start from. SamoBot checks the site first and shows you its map, so you can choose what to fetch.',
  },
  checking: {
    title: 'Checking the site',
    text: 'SamoBot reads robots.txt, the sitemap and the start page. Nothing else is fetched yet.',
  },
}

export function RequestCrawlDialog({ open, onOpenChange, initialUrl }: RequestCrawlDialogProps) {
  const [phase, setPhase] = useState<DialogPhase>('start')
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[92vh] max-w-xl overflow-y-auto">
        <DialogHeader className="gap-2 text-left">
          <p className="label">New crawl</p>
          <DialogTitle className="text-[22px] leading-7 tracking-[-0.01em]">{COPY[phase].title}</DialogTitle>
          <DialogDescription>{COPY[phase].text}</DialogDescription>
        </DialogHeader>
        <CheckSiteForm initialUrl={initialUrl} onDone={() => onOpenChange(false)} onPhase={setPhase} />
      </DialogContent>
    </Dialog>
  )
}
