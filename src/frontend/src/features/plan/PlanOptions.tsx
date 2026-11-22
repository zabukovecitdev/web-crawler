import { Segmented } from '@/components/shared/Segmented'
import { Card } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'
import { cn } from '@/lib/utils'
import type { RenderMode, SiteCheckDto } from '@/types/api'
import { renderHint } from './render-hint'

const MODES = [
  { value: 'Auto', label: 'Auto-detect' },
  { value: 'Always', label: 'Always' },
  { value: 'Never', label: 'Never' },
] as const

type Props = {
  hasSitemap: boolean
  linked: boolean
  review: boolean
  mode: RenderMode
  check: SiteCheckDto
  browserUp: boolean
  onLinked: (value: boolean) => void
  onReview: (value: boolean) => void
  onMode: (mode: RenderMode) => void
}

function LinkOption({ props }: { props: Props }) {
  const sitemap = props.hasSitemap
  return (
    <Card className="flex flex-col gap-3 p-5 shadow-zb-1">
      <p className="label">{sitemap ? 'Pages not in the sitemap' : 'Review'}</p>
      <div className="flex items-start gap-3">
        <Switch
          id="plan-option"
          aria-labelledby="plan-option-label"
          checked={sitemap ? props.linked : props.review}
          onCheckedChange={sitemap ? props.onLinked : props.onReview}
        />
        <div className="flex flex-col gap-0.5">
          <span id="plan-option-label" className="text-[14px] font-semibold">
            {sitemap ? 'Also fetch pages that are linked but not in the sitemap' : 'Pause after the first 100 pages so I can review the map'}
          </span>
          <span className="text-[13px] text-muted-foreground">
            {sitemap
              ? 'They join the folder they belong to and follow its tick.'
              : 'By then most folders are known, and you can untick what you don’t need.'}
          </span>
        </div>
      </div>
      <p className="m-0 text-[13px] text-muted-foreground">
        There is no page limit to guess. If one folder keeps growing far beyond what you selected, you can untick it while the crawl runs.
      </p>
    </Card>
  )
}

export function PlanOptions(props: Props) {
  const hint = renderHint(props.mode, props.check.javaScript.verdict, props.browserUp)
  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
      <LinkOption props={props} />
      <Card className="flex flex-col gap-3 p-5 shadow-zb-1">
        <p className="label">How pages are fetched</p>
        <span id="plan-render-label" className="text-[14px] font-semibold">
          Render JavaScript
        </span>
        <Segmented label="Render JavaScript" options={[...MODES]} value={props.mode} onChange={props.onMode} />
        <p
          className={cn(
            'm-0 text-[13px]',
            hint.tone === 'muted' && 'text-muted-foreground',
            hint.tone === 'error' && 'text-signal-ink',
            hint.tone === 'warn' && 'text-amber-ink',
          )}
        >
          {hint.text}
        </p>
      </Card>
    </div>
  )
}
