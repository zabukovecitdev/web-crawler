import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { DialogFooter } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { validateUrl } from '../crawl/crawl-validation'
import { SiteCheckList } from './SiteCheckList'
import { checkRows, waitingRows } from './check-rows'
import { checkSite, errorMessage, planKeys } from './plan-api'
import type { SiteCheckDto } from '@/types/api'

export type DialogPhase = 'start' | 'checking'

type Props = { initialUrl?: string; onDone: () => void; onPhase: (phase: DialogPhase) => void }

function UrlField({ value, error, onChange }: { value: string; error: string | null; onChange: (value: string) => void }) {
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor="crawl-url">Start URL</Label>
      <Input
        id="crawl-url"
        type="url"
        autoComplete="url"
        placeholder="https://docs.example.com/guide/install"
        value={value}
        aria-invalid={error ? true : undefined}
        aria-describedby="crawl-url-hint"
        onChange={(event) => onChange(event.target.value)}
      />
      <p id="crawl-url-hint" className={error ? 'm-0 text-[13px] text-signal-ink' : 'm-0 text-[13px] text-muted-foreground'}>
        {error ?? 'Include https://. The crawl stays on this site. Nothing is fetched until you have seen the site map and started the crawl.'}
      </p>
    </div>
  )
}

function useSiteCheckMutation() {
  return useMutation({ mutationFn: checkSite })
}

function seeMap(check: SiteCheckDto): boolean {
  return check.startPage.reachable
}

export function CheckSiteForm({ initialUrl, onDone, onPhase }: Props) {
  const [url, setUrl] = useState(initialUrl ?? '')
  const [submitted, setSubmitted] = useState(false)
  const check = useSiteCheckMutation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const error = submitted ? validateUrl(url) : null
  const checking = check.isPending || Boolean(check.data)
  useEffect(() => onPhase(checking ? 'checking' : 'start'), [checking, onPhase])

  const submit = (event: FormEvent) => {
    event.preventDefault()
    setSubmitted(true)
    if (!validateUrl(url)) {
      check.mutate(url.trim())
    }
  }
  const toPlan = (result: SiteCheckDto) => {
    queryClient.setQueryData(planKeys.check(result.requestedUrl), result)
    onDone()
    navigate(`/crawls/new?url=${encodeURIComponent(result.requestedUrl)}`)
  }

  if (checking) {
    return (
      <div className="flex flex-col gap-5">
        <SiteCheckList rows={check.data ? checkRows(check.data) : waitingRows()} />
        <DialogFooter className="gap-2 sm:gap-0">
          <Button type="button" variant="ghost" onClick={() => check.reset()} disabled={check.isPending}>
            Back
          </Button>
          <Button type="button" disabled={!check.data || !seeMap(check.data)} onClick={() => check.data && toPlan(check.data)}>
            See the site map
          </Button>
        </DialogFooter>
      </div>
    )
  }
  return (
    <form noValidate onSubmit={submit} className="flex flex-col gap-5">
      <UrlField value={url} error={error} onChange={setUrl} />
      {check.isError ? (
        <p role="alert" className="m-0 rounded-md bg-signal-100 p-3 text-[13px] leading-[18px] text-signal-ink">
          <strong>The site could not be checked.</strong> {errorMessage(check.error)}
        </p>
      ) : null}
      <DialogFooter className="gap-2 sm:gap-0">
        <Button type="button" variant="ghost" onClick={onDone}>
          Cancel
        </Button>
        <Button type="submit">Check site</Button>
      </DialogFooter>
    </form>
  )
}
