import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import type { CrawlOptions, OptionErrors } from './crawl-validation'

type Props = {
  options: CrawlOptions
  errors: OptionErrors
  disabled: boolean
  onChange: (patch: Partial<CrawlOptions>) => void
}

type NumberFieldProps = {
  id: string
  label: string
  hint: string
  value: string
  error: string | null
  disabled: boolean
  onChange: (value: string) => void
}

function NumberField({ id, label, hint, value, error, disabled, onChange }: NumberFieldProps) {
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type="number"
        inputMode="numeric"
        min={0}
        value={value}
        disabled={disabled}
        aria-invalid={error ? true : undefined}
        aria-describedby={`${id}-hint`}
        onChange={(event) => onChange(event.target.value)}
      />
      <p id={`${id}-hint`} className={error ? 'text-[13px] text-signal-ink' : 'text-[13px] text-muted-foreground'}>
        {error ?? hint}
      </p>
    </div>
  )
}

type ToggleProps = { id: string; label: string; checked: boolean; disabled: boolean; onChange: (checked: boolean) => void }

function Toggle({ id, label, checked, disabled, onChange }: ToggleProps) {
  return (
    <div className="flex items-center gap-3">
      <Switch id={id} checked={checked} disabled={disabled} onCheckedChange={onChange} />
      <Label htmlFor={id} className="text-sm font-normal">
        {label}
      </Label>
    </div>
  )
}

/** The crawl options, set apart from the address by a rule and an engraved label. */
export function CrawlOptionsFields({ options, errors, disabled, onChange }: Props) {
  return (
    <fieldset className="m-0 flex min-w-0 flex-col gap-4 border-0 border-t p-0 pt-5">
      <legend className="label float-left mb-4 w-full">Options</legend>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <NumberField
          id="crawl-depth"
          label="Maximum depth"
          hint="Links away from the first page."
          value={options.maxDepth}
          error={errors.maxDepth}
          disabled={disabled}
          onChange={(maxDepth) => onChange({ maxDepth })}
        />
        <NumberField
          id="crawl-pages"
          label="Maximum pages"
          hint="The crawl stops at this number."
          value={options.maxUrls}
          error={errors.maxUrls}
          disabled={disabled}
          onChange={(maxUrls) => onChange({ maxUrls })}
        />
      </div>
      <Toggle id="crawl-robots" label="Respect robots.txt" checked={options.respectRobots} disabled={disabled} onChange={(respectRobots) => onChange({ respectRobots })} />
      <Toggle id="crawl-js" label="Render JavaScript in a browser (slower)" checked={options.useJsRendering} disabled={disabled} onChange={(useJsRendering) => onChange({ useJsRendering })} />
    </fieldset>
  )
}
