import { useId, useState, type FormEvent } from 'react'
import { ToneBadge } from '@/components/shared/StatusBadge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import type { ExplainDto } from '@/types/api'
import { useExplain } from './sitemap-api'

function Result({ result }: { result: ExplainDto }) {
  return (
    <p className="m-0 flex flex-wrap items-center gap-2 text-[13px]">
      <ToneBadge tone={result.allowedByRobots ? 'success' : 'warning'}>
        {result.allowedByRobots ? 'Allowed' : 'Blocked'}
      </ToneBadge>
      {result.matchedRule ? (
        <span>
          The rule <span className="readout">{result.matchedRule}</span> applies to this URL for SamoBot.
        </span>
      ) : (
        <span>No rule in robots.txt blocks this URL for SamoBot.</span>
      )}
    </p>
  )
}

/** "Test a URL" against the host's robots.txt. */
export function ExplainForm({ host }: { host: string }) {
  const id = useId()
  const [url, setUrl] = useState(`https://${host}/`)
  const explain = useExplain()

  function submit(event: FormEvent) {
    event.preventDefault()
    explain.mutate(url.trim())
  }

  return (
    <form onSubmit={submit} className="flex flex-col gap-3">
      <label htmlFor={id} className="label">
        Test a URL
      </label>
      <div className="flex flex-wrap gap-3">
        <Input
          id={id}
          type="url"
          required
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          aria-label="URL to test against robots.txt"
          className="min-w-0 flex-1 basis-60"
        />
        <Button type="submit" variant="outline" disabled={explain.isPending}>
          Test
        </Button>
      </div>
      <div role="status">
        {explain.data ? <Result result={explain.data} /> : null}
        {explain.isError ? <p className="m-0 text-[13px] text-signal-ink">The test failed. Try again.</p> : null}
      </div>
    </form>
  )
}
