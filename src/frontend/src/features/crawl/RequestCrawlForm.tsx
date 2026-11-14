import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { DialogFooter } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { CrawlOptionsFields } from './CrawlOptionsFields'
import { createJob, crawlKeys, errorMessage } from './crawl-api'
import { buildRequest, DEFAULT_OPTIONS, validateOptions, validateUrl, type CrawlOptions } from './crawl-validation'

type Props = { initialUrl?: string; onDone: () => void }

function useCreateCrawl(onDone: () => void) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createJob,
    onSuccess: (job) => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
      onDone()
      navigate(`/crawls/${job.id}`)
    },
  })
}

type UrlFieldProps = { value: string; error: string | null; disabled: boolean; onChange: (value: string) => void }

function UrlField({ value, error, disabled, onChange }: UrlFieldProps) {
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor="crawl-url">Site URL</Label>
      <Input
        id="crawl-url"
        type="url"
        autoComplete="url"
        placeholder="https://docs.example.com"
        value={value}
        disabled={disabled}
        aria-invalid={error ? true : undefined}
        aria-describedby="crawl-url-hint"
        onChange={(event) => onChange(event.target.value)}
      />
      <p id="crawl-url-hint" className={error ? 'text-[13px] text-signal-ink' : 'text-[13px] text-muted-foreground'}>
        {error ?? 'Include https://. One site for each crawl.'}
      </p>
    </div>
  )
}

export function RequestCrawlForm({ initialUrl, onDone }: Props) {
  const [url, setUrl] = useState(initialUrl ?? '')
  const [options, setOptions] = useState<CrawlOptions>(DEFAULT_OPTIONS)
  const [submitted, setSubmitted] = useState(false)
  const create = useCreateCrawl(onDone)
  const urlError = submitted ? validateUrl(url) : null
  const optionErrors = validateOptions(options)

  const submit = (event: FormEvent) => {
    event.preventDefault()
    setSubmitted(true)
    if (!validateUrl(url) && !optionErrors.maxDepth && !optionErrors.maxUrls) {
      create.mutate(buildRequest(url, options))
    }
  }

  return (
    <form noValidate onSubmit={submit} className="flex flex-col gap-5">
      <UrlField value={url} error={urlError} disabled={create.isPending} onChange={setUrl} />
      <CrawlOptionsFields
        options={options}
        errors={optionErrors}
        disabled={create.isPending}
        onChange={(patch) => setOptions((current) => ({ ...current, ...patch }))}
      />
      {create.isError ? (
        <p role="alert" className="m-0 rounded-md bg-signal-100 p-3 text-[13px] leading-[18px] text-signal-ink">
          <strong>The crawl was not started.</strong> {errorMessage(create.error)}
        </p>
      ) : null}
      <DialogFooter className="gap-2 sm:gap-0">
        <Button type="button" variant="ghost" onClick={onDone} disabled={create.isPending}>
          Cancel
        </Button>
        <Button type="submit" disabled={create.isPending}>
          {create.isPending ? 'Starting…' : 'Start crawl'}
        </Button>
      </DialogFooter>
    </form>
  )
}
