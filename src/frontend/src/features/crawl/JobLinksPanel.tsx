import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { EmptyState, ErrorState, LoadingRows } from '@/components/shared/States'
import { Input } from '@/components/ui/input'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatNumber } from '@/lib/format'
import type { CrawlJobDto, JobLinkDto, JobLinkFilter } from '@/types/api'
import { Pager } from '../sites/Pager'
import { useDebounced } from '../sites/useDebounced'
import { JobLinksTable } from './JobLinksTable'
import { NewCrawlForLinkDialog } from './NewCrawlForLinkDialog'
import { crawlKeys, errorMessage, fetchLinks, recrawlLink } from './crawl-api'
import { pendingLinks } from './link-format'

const PAGE_SIZE = 25

function filtersOf(job: CrawlJobDto): { value: JobLinkFilter; label: string }[] {
  const { links } = job
  return [
    { value: 'all', label: `All ${formatNumber(links.total)}` },
    { value: 'problems', label: `Problems ${formatNumber(links.failed + links.retrying)}` },
    { value: 'notIndexed', label: `Not indexed ${formatNumber(links.total - links.indexed)}` },
    { value: 'pending', label: `In progress ${formatNumber(pendingLinks(links))}` },
    { value: 'indexed', label: `Indexed ${formatNumber(links.indexed)}` },
  ]
}

function useLinks(job: CrawlJobDto, filter: JobLinkFilter, search: string, offset: number) {
  const query = { filter, query: useDebounced(search), limit: PAGE_SIZE, offset }
  return useQuery({
    queryKey: crawlKeys.links(job.id, query),
    queryFn: () => fetchLinks(job.id, query),
    placeholderData: keepPreviousData,
    refetchInterval: job.status === 'Running' ? 4000 : false,
  })
}

function useRecrawlLink(jobId: number) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (linkId: number) => recrawlLink(jobId, linkId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.job(jobId) })
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
    },
  })
}

type ToolbarProps = {
  job: CrawlJobDto
  filter: JobLinkFilter
  search: string
  onFilter: (filter: JobLinkFilter) => void
  onSearch: (search: string) => void
}

function Toolbar({ job, filter, search, onFilter, onSearch }: ToolbarProps) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <Tabs value={filter} onValueChange={(value) => onFilter(value as JobLinkFilter)}>
        <TabsList aria-label="Which links to show" className="h-auto flex-wrap">
          {filtersOf(job).map((item) => (
            <TabsTrigger key={item.value} value={item.value}>
              {item.label}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>
      <Input
        type="search"
        aria-label="Find a link by address or title"
        placeholder="Find a link"
        className="w-full sm:w-56"
        value={search}
        onChange={(event) => onSearch(event.target.value)}
      />
    </div>
  )
}

/** The links of a job with their state, filtered and paged, each with a recrawl button. */
export function JobLinksPanel({ job }: { job: CrawlJobDto }) {
  const [filter, setFilter] = useState<JobLinkFilter>('all')
  const [search, setSearch] = useState('')
  const [offset, setOffset] = useState(0)
  const links = useLinks(job, filter, search, offset)
  const recrawl = useRecrawlLink(job.id)
  const [asNewCrawl, setAsNewCrawl] = useState<JobLinkDto | null>(null)
  // A cancelled crawl is not run again for one link: offer a new crawl of that link instead.
  const onRecrawl = (link: JobLinkDto) => (job.status === 'Cancelled' ? setAsNewCrawl(link) : recrawl.mutate(link.id))
  const restart = <T,>(set: (value: T) => void) => (value: T) => {
    set(value)
    setOffset(0)
  }
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <Toolbar job={job} filter={filter} search={search} onFilter={restart(setFilter)} onSearch={restart(setSearch)} />
      {recrawl.isError ? (
        <p role="alert" className="m-0 text-[13px] text-signal-ink">
          <strong>Not queued:</strong> {errorMessage(recrawl.error)}
        </p>
      ) : null}
      {links.isError ? <ErrorState error={links.error} onRetry={() => void links.refetch()} /> : null}
      {links.isPending ? <LoadingRows rows={4} /> : null}
      {links.data?.items.length === 0 ? <EmptyState title="No links match">Choose another filter or clear the search.</EmptyState> : null}
      {links.data && links.data.items.length > 0 ? (
        <>
          <JobLinksTable links={links.data.items} busy={recrawl.isPending} onRecrawl={onRecrawl} />
          <Pager total={links.data.total} offset={offset} limit={PAGE_SIZE} shown={links.data.items.length} onChange={setOffset} />
        </>
      ) : null}
      <NewCrawlForLinkDialog job={job} link={asNewCrawl} onClose={() => setAsNewCrawl(null)} />
    </div>
  )
}
