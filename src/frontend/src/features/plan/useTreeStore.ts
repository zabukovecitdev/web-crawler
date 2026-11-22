import { useQueries } from '@tanstack/react-query'
import { useState } from 'react'
import type { PathTreeDto } from '@/types/api'
import { buildStore, folderPrefixes, providedParents, type TreeLevel, type TreeStore } from './tree-store'

export const EXPAND_LIMIT = 30

export type TreeSource = {
  keyOf: (parent: string) => readonly unknown[]
  fetchTree: (parent: string) => Promise<PathTreeDto>
  refetchMs: number | false
  enabled: boolean
}

export type TreeStoreState = {
  store: TreeStore
  open: ReadonlySet<string>
  loading: boolean
  error: Error | null
  toggle: (prefix: string) => void
  expandAll: () => void
  collapseAll: () => void
  reload: () => void
}

function useTreeQueries(source: TreeSource, parents: string[]) {
  return useQueries({
    queries: parents.map((parent) => ({
      queryKey: source.keyOf(parent),
      queryFn: () => source.fetchTree(parent),
      refetchInterval: source.refetchMs,
      enabled: source.enabled,
      staleTime: 10_000,
    })),
  })
}

function toggled(current: ReadonlySet<string>, prefix: string): Set<string> {
  const next = new Set(current)
  return next.delete(prefix) ? next : next.add(prefix)
}

export function useTreeStore(source: TreeSource): TreeStoreState {
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set())
  const [queried, setQueried] = useState<string[]>(['/'])
  const results = useTreeQueries(source, queried)
  const levels: TreeLevel[] = results.flatMap((result) => (result.data ? [{ data: result.data, updatedAt: result.dataUpdatedAt }] : []))
  const store = buildStore(levels)
  const provided = providedParents(levels)
  const want = (prefixes: string[]) =>
    setQueried((current) => [...new Set([...current, ...prefixes.filter((prefix) => !provided.has(prefix))])])
  const folders = folderPrefixes(store)
  return {
    store,
    open,
    loading: results[0].isPending && source.enabled,
    error: results[0].error,
    toggle: (prefix) => {
      setOpen((current) => toggled(current, prefix))
      want([prefix])
    },
    expandAll: () => {
      setOpen(new Set(folders))
      want(folders.slice(0, EXPAND_LIMIT))
    },
    collapseAll: () => setOpen(new Set()),
    reload: () => void results[0].refetch(),
  }
}
