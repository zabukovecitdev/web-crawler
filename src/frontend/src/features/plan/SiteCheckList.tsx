import { cn } from '@/lib/utils'
import type { CheckRow, CheckTone } from './check-rows'

const MARK: Record<CheckTone, { icon: string; box: string; word: string }> = {
  ok: { icon: '✓', box: 'bg-led-soft text-led-ink', word: 'Passed' },
  warn: { icon: '!', box: 'bg-amber-soft text-amber-ink', word: 'Warning' },
  err: { icon: '×', box: 'bg-signal-100 text-signal-ink', word: 'Problem' },
  info: { icon: 'i', box: 'bg-muted text-foreground', word: 'Note' },
  pending: { icon: '…', box: 'bg-muted text-muted-foreground', word: 'Working' },
}

export function SiteCheckList({ rows, columns = false }: { rows: CheckRow[]; columns?: boolean }) {
  return (
    <ul aria-live="polite" className={cn('m-0 grid list-none gap-4 p-0', columns && 'md:grid-cols-2')}>
      {rows.map((row) => (
        <li key={row.name} className="flex items-start gap-3">
          <span aria-hidden="true" className={cn('mt-0.5 flex size-5 shrink-0 items-center justify-center rounded-full text-xs font-semibold', MARK[row.tone].box)}>
            {MARK[row.tone].icon}
          </span>
          <div className="flex min-w-0 flex-col gap-0.5">
            <p className="m-0 text-[14px] font-semibold leading-5">
              {row.name}
              <span className="sr-only"> ({MARK[row.tone].word})</span>
            </p>
            <p className="m-0 text-[13px] leading-[18px] text-muted-foreground">{row.text}</p>
          </div>
        </li>
      ))}
    </ul>
  )
}
