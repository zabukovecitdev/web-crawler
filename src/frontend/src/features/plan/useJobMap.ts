import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import type { CrawlJobDto, CrawlJobStatus, PathRule } from '@/types/api'
import { crawlKeys } from '../crawl/crawl-api'
import { fetchJobTree, planKeys, putPathRules } from './plan-api'
import { pathOf } from './path-rules'
import { runRowView, type RowView } from './row-view'
import { buildRowViews } from './tree-view'
import { folderPrefixes } from './tree-store'
import type { TreeFilter } from './tree-rows'
import { useTreeStore, type TreeStoreState } from './useTreeStore'

export const JOB_TREE_POLL_MS = 5000

const EDITABLE: CrawlJobStatus[] = ['Pending', 'Running', 'Paused']

export type JobMap = {
  tree: TreeStoreState
  rows: RowView[]
  startPath: string
  editable: boolean
  saving: boolean
  saveError: Error | null
  allOpen: boolean
  query: string
  setQuery: (query: string) => void
  filter: TreeFilter
  setFilter: (filter: TreeFilter) => void
  setRules: (rules: PathRule[]) => void
  rules: PathRule[]
}

function userRulesOf(job: CrawlJobDto, startPath: string): PathRule[] {
  return job.pathRules.filter((rule) => !(rule.exact && rule.kind === 'Include' && rule.prefix === startPath))
}

function useSavedRules(job: CrawlJobDto, startPath: string) {
  const queryClient = useQueryClient()
  const [rules, setRules] = useState<PathRule[]>(() => userRulesOf(job, startPath))
  const save = useMutation({
    mutationFn: (next: PathRule[]) => putPathRules(job.id, next),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['crawl', 'job', job.id] })
      void queryClient.invalidateQueries({ queryKey: crawlKeys.jobs })
    },
    onError: () => setRules(userRulesOf(job, startPath)),
  })
  return { rules, save, change: (next: PathRule[]) => { setRules(next); save.mutate(next) } }
}

export function useJobMap(job: CrawlJobDto, status: CrawlJobStatus): JobMap {
  const startPath = pathOf(job.seedUrls[0] ?? 'https://invalid.example/')
  const [query, setQuery] = useState('')
  const [filter, setFilter] = useState<TreeFilter>('all')
  const saved = useSavedRules(job, startPath)
  const active = EDITABLE.includes(status)
  const tree = useTreeStore({ keyOf: (parent) => planKeys.jobTree(job.id, parent), fetchTree: (parent) => fetchJobTree(job.id, parent), refetchMs: active ? JOB_TREE_POLL_MS : false, enabled: true })
  const inBrowser = job.renderMode === 'Always' || (job.renderMode === 'Auto' && job.jsNeeded)
  const toView = (row: Parameters<typeof runRowView>[0]) => runRowView(row, { job: { status, pauseReason: job.pauseReason }, startPath, inBrowser, editable: active })
  const rows = buildRowViews({ store: tree.store, open: tree.open, rules: saved.rules, startPath, query, filter, toView })
  const folders = folderPrefixes(tree.store)
  return {
    tree, rows, startPath, editable: active, saving: saved.save.isPending, saveError: saved.save.error, query, setQuery, filter, setFilter,
    setRules: saved.change, rules: saved.rules, allOpen: folders.length > 0 && folders.every((prefix) => tree.open.has(prefix)),
  }
}
