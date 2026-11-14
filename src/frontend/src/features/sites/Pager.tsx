import { Button } from '@/components/ui/button'
import { formatNumber } from '@/lib/format'

type Props = { total: number; offset: number; limit: number; shown: number; onChange: (offset: number) => void }

/** "1–8 of 4 812" with Previous and Next. */
export function Pager({ total, offset, limit, shown, onChange }: Props) {
  if (total === 0) {
    return null
  }
  return (
    <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
      <p className="readout m-0 text-muted-foreground" aria-live="polite">
        {formatNumber(offset + 1)}–{formatNumber(offset + shown)} of {formatNumber(total)}
      </p>
      <div className="flex gap-2">
        <Button variant="outline" size="sm" disabled={offset === 0} onClick={() => onChange(Math.max(0, offset - limit))}>
          Previous
        </Button>
        <Button variant="outline" size="sm" disabled={offset + limit >= total} onClick={() => onChange(offset + limit)}>
          Next
        </Button>
      </div>
    </div>
  )
}
