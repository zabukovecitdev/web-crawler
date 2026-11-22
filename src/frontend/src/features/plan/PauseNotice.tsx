import { Notice } from '@/components/shared/Notice'
import { Button } from '@/components/ui/button'
import type { CrawlJobDto, CrawlJobStatus } from '@/types/api'
import { useJobAction } from '../crawl/useJobAction'
import { formatNumber } from './plan-summary'

type Props = { job: CrawlJobDto; status: CrawlJobStatus; fetched: number; found: number }

export function PauseNotice({ job, status, fetched, found }: Props) {
  const action = useJobAction(job.id)
  if (status !== 'Paused') {
    return null
  }
  if (job.pauseReason === 'browser-unavailable') {
    return (
      <Notice tone="warn" title="Waiting for the browser service.">
        <p className="m-0">
          Pages of this crawl are rendered in a browser, so it paused at {formatNumber(fetched)} of {formatNumber(found)}. It goes on by itself when the
          service answers again. Nothing is lost.
        </p>
      </Notice>
    )
  }
  if (job.pauseReason === 'review') {
    return (
      <Notice
        tone="info"
        title="Review the map."
        actions={
          <Button size="sm" disabled={action.isPending} onClick={() => action.mutate('start')}>
            {action.isPending ? 'Continuing…' : 'Continue crawl'}
          </Button>
        }
      >
        <p className="m-0">
          {formatNumber(fetched)} pages fetched and {formatNumber(found)} found so far. Untick the folders you don’t need below. New pages in unticked
          folders won’t be fetched.
        </p>
      </Notice>
    )
  }
  return null
}
