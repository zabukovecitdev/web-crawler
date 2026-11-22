import { ChevronRight } from 'lucide-react'
import { cn } from '@/lib/utils'
import { nextChecked } from './path-rules'
import { RowStatusBadge } from './RowStatusBadge'
import type { RowView } from './row-view'
import { TriCheckbox } from './TriCheckbox'

type Props = { row: RowView; onToggleOpen: (prefix: string) => void; onSetChecked: (row: RowView, checked: boolean) => void }

const INDENT_REM = 1.25

function Bar({ row }: { row: RowView }) {
  return (
    <div aria-hidden="true" className="hidden h-2 overflow-hidden rounded-full bg-muted md:flex">
      <i className="block h-full bg-chart-3" style={{ width: `${row.doneShare}%` }} />
      <i className="block h-full bg-signal-400" style={{ width: `${row.failedShare}%` }} />
    </div>
  )
}

function Name({ row, onToggleOpen }: Pick<Props, 'row' | 'onToggleOpen'>) {
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
      {row.expandable ? (
        <button
          type="button"
          aria-expanded={row.open}
          aria-label={`${row.open ? 'Close' : 'Open'} ${row.label}`}
          onClick={() => onToggleOpen(row.node.prefix)}
          className="flex size-6 shrink-0 items-center justify-center rounded-md hover:bg-accent"
        >
          <ChevronRight aria-hidden="true" className={cn('size-3.5 transition-transform', row.open && 'rotate-90')} />
        </button>
      ) : (
        <span aria-hidden="true" className="size-6 shrink-0" />
      )}
      <span className="readout min-w-0 [overflow-wrap:anywhere]">{row.label}</span>
      {row.chips.map((chip) => (
        <span key={chip} className="rounded-sm bg-muted px-1.5 py-0.5 text-[11px] font-medium text-muted-foreground">
          {chip}
        </span>
      ))}
    </div>
  )
}

export function TreeRow({ row, onToggleOpen, onSetChecked }: Props) {
  const verb = row.state === 'true' ? 'Untick' : 'Tick'
  return (
    <li className={cn('grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4 gap-y-1 border-b px-3 py-2 last:border-b-0 md:grid-cols-[minmax(0,1fr)_120px_110px_110px_170px]', row.state === 'false' && 'text-muted-foreground')}>
      <div className="flex min-w-0 items-start gap-1" style={{ paddingLeft: `${row.level * INDENT_REM}rem` }}>
        <TriCheckbox checked={row.state} label={`${verb} ${row.label}`} disabled={row.disabled} onToggle={() => onSetChecked(row, nextChecked(row.state))} />
        <div className="flex min-w-0 flex-col gap-0.5">
          <Name row={row} onToggleOpen={onToggleOpen} />
          {row.note ? <p className="m-0 pl-8 text-[13px] leading-[18px] text-muted-foreground">{row.note}</p> : null}
        </div>
      </div>
      <Bar row={row} />
      <span className="readout text-right">{row.count}</span>
      <span className="hidden text-right text-[13px] text-muted-foreground md:block">{row.updated}</span>
      <span className="col-span-2 flex justify-end md:col-span-1">
        <RowStatusBadge status={row.status} />
      </span>
    </li>
  )
}
