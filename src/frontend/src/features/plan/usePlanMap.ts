import type { PathRule, SiteCheckDto } from '@/types/api'
import { fetchPlanTree, planKeys } from './plan-api'
import { pathOf, withPin } from './path-rules'
import { tickedPages, totalPages } from './plan-totals'
import { planRowView, type RowView } from './row-view'
import { buildRowViews, startNode } from './tree-view'
import type { TreeFilter } from './tree-rows'
import { useTreeStore, type TreeStoreState } from './useTreeStore'
import { folderPrefixes, type TreeStore } from './tree-store'

export type PlanMap = {
  tree: TreeStoreState
  store: TreeStore
  rows: RowView[]
  startPath: string
  ticked: number
  total: number
  allOpen: boolean
}

export type PlanMapInput = { rules: PathRule[]; query: string; filter: TreeFilter }

export function usePlanMap(check: SiteCheckDto, hasSitemap: boolean, input: PlanMapInput): PlanMap {
  const tree = useTreeStore({
    keyOf: (parent) => planKeys.tree(check.host, parent),
    fetchTree: (parent) => fetchPlanTree(check.host, parent),
    refetchMs: false,
    enabled: hasSitemap,
  })
  const startPath = pathOf(check.startUrl)
  const store: TreeStore = hasSitemap ? tree.store : { '/': [startNode(startPath)] }
  const { rules } = input
  const toView = (row: Parameters<typeof planRowView>[0]) => planRowView(row, { store, rules, startPath, hasSitemap })
  const rows = buildRowViews({ store, open: tree.open, rules, startPath, query: input.query, filter: input.filter, toView })
  const folders = folderPrefixes(store)
  return {
    tree,
    store,
    rows,
    startPath,
    ticked: tickedPages(store, withPin(rules, startPath)),
    total: totalPages(store),
    allOpen: folders.length > 0 && folders.every((prefix) => tree.open.has(prefix)),
  }
}
