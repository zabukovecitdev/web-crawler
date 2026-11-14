import { Link } from 'react-router-dom'
import { buttonVariants } from '@/components/ui/button-variants'
import { Card } from '@/components/ui/card'
import { formatNumber } from '@/lib/format'
import type { CrawlProgress } from '@/types/api'

const HEADLINES = {
  Completed: 'This crawl is complete.',
  Cancelled: 'This crawl was cancelled.',
  Failed: 'This crawl failed.',
} as const

function headline(status: CrawlProgress['status']): string {
  return status in HEADLINES ? HEADLINES[status as keyof typeof HEADLINES] : 'This crawl has ended.'
}

type Props = { progress: CrawlProgress; host: string; elapsed: string }

/** Shown instead of the live indicator once the job has ended. */
export function FinalSummary({ progress, host, elapsed }: Props) {
  return (
    <Card className="flex flex-wrap items-center justify-between gap-4 p-5 shadow-zb-1">
      <div className="flex flex-col gap-1">
        <p className="label">Summary</p>
        <p className="m-0 text-base font-semibold">{headline(progress.status)}</p>
        <p className="m-0 text-[13px] leading-[18px] text-muted-foreground">
          {formatNumber(progress.crawled)} pages crawled, {formatNumber(progress.failed)} failed, in {elapsed}.
        </p>
      </div>
      <Link to={`/sites/${encodeURIComponent(host)}`} className={buttonVariants({ variant: 'outline' })}>
        View site {host}
      </Link>
    </Card>
  )
}
