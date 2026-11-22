import type { PathTreeDto, TreeCountsDto, TreeNodeDto } from '@/types/api'

export type TreeStore = Record<string, TreeNodeDto[]>

export type TreeLevel = { data: PathTreeDto; updatedAt: number }

export function emptyCounts(): TreeCountsDto {
  return { pages: 0, done: 0, failed: 0, skipped: 0, stopped: 0, fetching: 0, queued: 0, oldestModified: null, newestModified: null }
}

function plain(node: TreeNodeDto): TreeNodeDto {
  return { ...node, children: null }
}

function entriesOf(level: TreeLevel): [string, TreeNodeDto[]][] {
  const own: [string, TreeNodeDto[]] = [level.data.parent, level.data.nodes.map(plain)]
  const nested = level.data.nodes
    .filter((node): node is TreeNodeDto & { children: TreeNodeDto[] } => node.children !== null)
    .map((node): [string, TreeNodeDto[]] => [node.prefix, node.children.map(plain)])
  return [own, ...nested]
}

export function buildStore(levels: TreeLevel[]): TreeStore {
  const store: TreeStore = {}
  const ordered = [...levels].sort((left, right) => left.updatedAt - right.updatedAt)
  for (const level of ordered) {
    for (const [parent, nodes] of entriesOf(level)) {
      store[parent] = nodes
    }
  }
  return store
}

export function providedParents(levels: TreeLevel[]): Set<string> {
  return new Set(levels.flatMap((level) => entriesOf(level).map(([parent]) => parent)))
}

export function folderPrefixes(store: TreeStore): string[] {
  return Object.values(store)
    .flat()
    .filter((node) => node.kind === 'Folder')
    .map((node) => node.prefix)
}

export function totalOf(store: TreeStore): TreeCountsDto {
  return (store['/'] ?? []).reduce(
    (sum, node) => ({ ...sum, pages: sum.pages + node.counts.pages }),
    emptyCounts(),
  )
}
