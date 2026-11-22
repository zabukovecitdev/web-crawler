import type { PathRule } from '@/types/api'
import { checkState, withPin } from './path-rules'
import type { RowView } from './row-view'
import { visibleRows, type RowModel, type TreeFilter } from './tree-rows'
import type { TreeStore } from './tree-store'

export type TreeViewInput = {
  store: TreeStore
  open: ReadonlySet<string>
  rules: PathRule[]
  startPath: string
  query: string
  filter: TreeFilter
  toView: (row: RowModel) => RowView
}

export function buildRowViews(input: TreeViewInput): RowView[] {
  const pinned = withPin(input.rules, input.startPath)
  const rows = visibleRows(input.store, {
    open: input.open,
    query: input.query,
    filter: input.filter,
    stateOf: (node) => checkState(pinned, node),
  })
  return rows.map(input.toView)
}

export function startNode(startPath: string) {
  return {
    prefix: startPath,
    kind: 'Page' as const,
    hasChildren: false,
    blocked: false,
    counts: { pages: 1, done: 0, failed: 0, skipped: 0, stopped: 0, fetching: 0, queued: 0, oldestModified: null, newestModified: null },
    children: null,
  }
}
