import { cn } from '@/lib/utils'

export type SegmentedOption<T extends string> = { value: T; label: string }

type Props<T extends string> = {
  label: string
  options: SegmentedOption<T>[]
  value: T
  onChange: (value: T) => void
  disabled?: boolean
}

export function Segmented<T extends string>({ label, options, value, onChange, disabled }: Props<T>) {
  return (
    <div role="group" aria-label={label} className="inline-flex w-fit self-start rounded-md border border-input bg-muted p-0.5">
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={option.value === value}
          disabled={disabled}
          onClick={() => onChange(option.value)}
          className={cn(
            'rounded-[4px] px-3 py-1 text-[13px] font-medium text-muted-foreground transition-colors hover:text-foreground disabled:opacity-50',
            option.value === value && 'bg-card text-foreground shadow-zb-1',
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}
