import { cn } from '@/lib/utils'
import type { Checked } from './path-rules'

type Props = { checked: Checked; label: string; disabled?: boolean; onToggle: () => void }

const MARKS: Record<Checked, string> = { true: '✓', mixed: '–', false: '' }

export function TriCheckbox({ checked, label, disabled, onToggle }: Props) {
  return (
    <button
      type="button"
      role="checkbox"
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      onClick={onToggle}
      className="flex size-6 shrink-0 items-center justify-center rounded-md disabled:cursor-not-allowed disabled:opacity-60"
    >
      <span
        aria-hidden="true"
        className={cn(
          'flex size-[18px] items-center justify-center rounded-[4px] border border-input bg-card text-[13px] font-semibold leading-none',
          checked !== 'false' && 'border-primary bg-primary text-primary-foreground',
        )}
      >
        {MARKS[checked]}
      </span>
    </button>
  )
}
