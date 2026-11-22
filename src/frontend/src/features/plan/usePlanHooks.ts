import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { createJob, crawlKeys } from '../crawl/crawl-api'
import type { BrowserStatusDto, PathRule, RenderMode, SiteCheckDto, SitemapPlanDto } from '@/types/api'
import { buildJobRequest, type Choices } from './job-request'
import { BROWSER_UNAVAILABLE, checkBrowserAgain, errorCode, fetchSitemapPlan, planKeys } from './plan-api'
import type { TreeFilter } from './tree-rows'

export const SITEMAP_POLL_MS = 3000
export const SITEMAP_WAIT_MS = 180_000

export type PlanChoices = {
  rules: PathRule[]
  setRules: (rules: PathRule[]) => void
  linked: boolean
  setLinked: (linked: boolean) => void
  review: boolean
  setReview: (review: boolean) => void
  mode: RenderMode
  setMode: (mode: RenderMode) => void
  query: string
  setQuery: (query: string) => void
  filter: TreeFilter
  setFilter: (filter: TreeFilter) => void
}

export function usePlanChoices(): PlanChoices {
  const [rules, setRules] = useState<PathRule[]>([])
  const [linked, setLinked] = useState(true)
  const [review, setReview] = useState(true)
  const [mode, setMode] = useState<RenderMode>('Auto')
  const [query, setQuery] = useState('')
  const [filter, setFilter] = useState<TreeFilter>('all')
  return { rules, setRules, linked, setLinked, review, setReview, mode, setMode, query, setQuery, filter, setFilter }
}

export function useSitemapPlan(check: SiteCheckDto): { plan: SitemapPlanDto; waitedTooLong: boolean } {
  const [since] = useState(() => Date.now())
  const query = useQuery({
    queryKey: planKeys.sitemap(check.host),
    queryFn: () => fetchSitemapPlan(check.host),
    initialData: check.sitemap,
    staleTime: SITEMAP_POLL_MS,
    refetchInterval: (current) => (current.state.data?.state === 'Pending' && Date.now() - since < SITEMAP_WAIT_MS ? SITEMAP_POLL_MS : false),
  })
  const plan = query.data
  return { plan, waitedTooLong: plan.state === 'Pending' && Date.now() - since >= SITEMAP_WAIT_MS }
}

export type BrowserState = {
  status: BrowserStatusDto
  checking: boolean
  checkedNote: string | null
  recheck: () => void
  reportDown: (reason: string | null) => void
}

export function useBrowserState(initial: BrowserStatusDto): BrowserState {
  const [status, setStatus] = useState(initial)
  const [note, setNote] = useState<string | null>(null)
  const recheck = useMutation({
    mutationFn: checkBrowserAgain,
    onSuccess: (next) => {
      setStatus(next)
      setNote(`Checked again at ${new Date(next.checkedAt).toTimeString().slice(0, 8)}. ${next.available ? 'The browser service is running.' : 'Still not available.'}`)
    },
  })
  return {
    status,
    checking: recheck.isPending,
    checkedNote: note,
    recheck: () => recheck.mutate(),
    reportDown: (reason) => setStatus({ available: false, reason, checkedAt: new Date().toISOString() }),
  }
}

export function useStartCrawl(check: SiteCheckDto, choices: Choices, onBrowserDown: (reason: string | null) => void) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: () => createJob(buildJobRequest(check, choices)),
    onSuccess: (job) => {
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
      navigate(`/crawls/${job.id}`)
    },
    onError: (error) => {
      if (errorCode(error) === BROWSER_UNAVAILABLE) {
        onBrowserDown(null)
      }
    },
  })
}
