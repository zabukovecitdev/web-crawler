import { Card } from '@/components/ui/card'
import { formatNumber } from '@/lib/format'
import { cn } from '@/lib/utils'

type Props = {
  hosts: { host: string; count: number }[]
  selected: string
  onSelect: (host: string) => void
}

type ButtonProps = { label: string; count: number; active: boolean; onClick: () => void }

function FacetButton({ label, count, active, onClick }: ButtonProps) {
  return (
    <li>
      <button
        type="button"
        onClick={onClick}
        aria-pressed={active}
        className={cn(
          'flex w-full items-center justify-between gap-3 rounded-md px-3 py-2 text-left text-[13px] hover:bg-muted',
          active && 'bg-muted font-semibold',
        )}
      >
        <span className="min-w-0 break-words">{label}</span>
        <span className="readout text-muted-foreground">{formatNumber(count)}</span>
      </button>
    </li>
  )
}

export function Facets({ hosts, selected, onSelect }: Props) {
  const all = hosts.reduce((sum, h) => sum + h.count, 0)
  return (
    <Card className="flex flex-col gap-3 p-4">
      <div className="flex flex-col gap-1">
        <p className="label">Filter</p>
        <h2 className="m-0 text-lg font-semibold">Sites</h2>
      </div>
      <ul className="m-0 flex list-none flex-col gap-1 p-0">
        <FacetButton label="All sites" count={all} active={!selected} onClick={() => onSelect('')} />
        {hosts.map((h) => (
          <FacetButton key={h.host} label={h.host} count={h.count} active={selected === h.host} onClick={() => onSelect(h.host)} />
        ))}
      </ul>
    </Card>
  )
}
