import type { Tone } from '@/components/shared/status'
import { formatNumber } from '@/lib/format'
import type { CrawlJobDto, JobLinkSummary, LinkState } from '@/types/api'

export type LinkLook = { tone: Tone; word: string; live: boolean }

const LINK_LOOKS: Record<LinkState, LinkLook> = {
  Queued: { tone: 'neutral', word: 'Queued', live: false },
  Fetching: { tone: 'success', word: 'Fetching', live: true },
  Retrying: { tone: 'warning', word: 'Retrying', live: false },
  Parsing: { tone: 'neutral', word: 'Parsing', live: false },
  Indexing: { tone: 'neutral', word: 'Indexing', live: false },
  Indexed: { tone: 'success', word: 'Indexed', live: false },
  NotIndexed: { tone: 'neutral', word: 'Not indexed', live: false },
  Skipped: { tone: 'neutral', word: 'Skipped', live: false },
  Failed: { tone: 'signal', word: 'Failed', live: false },
}

export function linkLook(state: LinkState): LinkLook {
  return LINK_LOOKS[state]
}

export type JobStep = { label: string; done: number; of: number; status: 'done' | 'active' | 'waiting'; note: string }

function stepStatus(done: number, of: number, started: boolean): JobStep['status'] {
  if (of > 0 && done >= of) {
    return 'done'
  }
  return started ? 'active' : 'waiting'
}

/** The four steps of a job as counts: how many links passed each step, out of how many that could. */
export function jobSteps(links: JobLinkSummary): JobStep[] {
  const stored = links.parsing + links.indexing + links.indexed + links.notIndexed
  const parsed = stored - links.parsing
  const fetched = links.total - links.queued - links.fetching - links.retrying
  const started = fetched > 0 || links.fetching > 0
  return [
    { label: 'Fetch', done: fetched, of: links.total, status: stepStatus(fetched, links.total, started), note: 'links requested' },
    { label: 'Store', done: stored, of: fetched, status: stepStatus(stored, stored, started), note: 'HTML pages kept' },
    { label: 'Parse', done: parsed, of: stored, status: stepStatus(parsed, stored, stored > 0), note: 'pages read' },
    { label: 'Index', done: links.indexed, of: parsed - links.notIndexed, status: stepStatus(links.indexed, parsed - links.notIndexed, parsed > 0), note: 'pages searchable' },
  ]
}

/** Links that still move on their own: queued, in flight, waiting for a retry, the parser or the index. */
export function pendingLinks(links: JobLinkSummary): number {
  return links.queued + links.fetching + links.retrying + links.parsing + links.indexing
}

function plural(count: number, word: string): string {
  return `${formatNumber(count)} ${word}${count === 1 ? '' : 's'}`
}

function runningHeadline(links: JobLinkSummary): string {
  const toFetch = links.queued + links.fetching + links.retrying
  if (toFetch > 0) {
    return `Fetching: ${plural(toFetch, 'link')} left in the queue.`
  }
  if (links.parsing > 0) {
    return `Parsing ${plural(links.parsing, 'stored page')}.`
  }
  return links.indexing > 0 ? `Sending ${plural(links.indexing, 'page')} to the search index.` : 'Finishing.'
}

function finishedHeadline(links: JobLinkSummary): string {
  const indexed = `${plural(links.indexed, 'page')} in the search index`
  return links.failed > 0 ? `Finished, but ${plural(links.failed, 'link')} failed. ${indexed}.` : `Finished. ${indexed}.`
}

const HEADLINES: Record<CrawlJobDto['status'], (links: JobLinkSummary) => string> = {
  Draft: () => 'Not started yet.',
  Pending: () => 'Not started yet.',
  Running: runningHeadline,
  Paused: (links) => `Paused with ${plural(pendingLinks(links), 'link')} still to do. Resume to continue.`,
  Completed: finishedHeadline,
  Cancelled: (links) => `Cancelled with ${plural(links.queued + links.retrying, 'link')} never fetched.`,
  Failed: () => 'The crawl failed.',
}

/** One sentence that says where the job stands, for the first thing a person reads. */
export function jobHeadline(job: Pick<CrawlJobDto, 'status' | 'links'>): string {
  return HEADLINES[job.status](job.links)
}

/** "HTTP 404 × 2, Timed out × 1", the reasons of the failed and retrying links. */
export function problemsText(links: JobLinkSummary): string {
  return links.problems.map((problem) => `${problem.label} × ${formatNumber(problem.count)}`).join(', ')
}
