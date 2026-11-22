import { describe, expect, it } from 'vitest'
import type { TreeNodeDto } from '@/types/api'
import type { Checked } from './path-rules'
import { labelOf, visibleRows, type RowContext } from './tree-rows'
import { emptyCounts, type TreeStore } from './tree-store'

const node = (prefix: string, kind: TreeNodeDto['kind'] = 'Folder'): TreeNodeDto => ({
  prefix,
  kind,
  hasChildren: kind === 'Folder',
  blocked: false,
  counts: { ...emptyCounts(), pages: 1 },
  children: null,
})

const STORE: TreeStore = {
  '/': [node('/guide/'), node('/blog/'), node('/changelog', 'Page')],
  '/guide/': [node('/guide/install', 'Page'), node('/guide/quickstart', 'Page')],
  '/blog/': [node('/blog/2025/'), node('/blog/2026/')],
}

function context(patch: Partial<RowContext> = {}): RowContext {
  return { open: new Set<string>(), query: '', filter: 'all', stateOf: () => 'true', ...patch }
}

const prefixes = (rows: ReturnType<typeof visibleRows>) => rows.map((row) => `${row.level}:${row.node.prefix}`)

describe('visibleRows', () => {
  it('shows the first level when nothing is open', () => {
    expect(prefixes(visibleRows(STORE, context()))).toEqual(['0:/guide/', '0:/blog/', '0:/changelog'])
  })

  it('shows the children of an open folder one level deeper', () => {
    const rows = visibleRows(STORE, context({ open: new Set(['/guide/']) }))

    expect(prefixes(rows)).toEqual(['0:/guide/', '1:/guide/install', '1:/guide/quickstart', '0:/blog/', '0:/changelog'])
    expect(rows[0].open).toBe(true)
  })

  it('finds paths in folders that are closed and opens the way to them', () => {
    expect(prefixes(visibleRows(STORE, context({ query: ' 2026 ' })))).toEqual(['0:/blog/', '1:/blog/2026/'])
  })

  it('shows everything inside a folder whose own name matches', () => {
    expect(prefixes(visibleRows(STORE, context({ query: 'guide' })))).toEqual(['0:/guide/', '1:/guide/install', '1:/guide/quickstart'])
  })

  it('shows nothing when no path matches', () => {
    expect(visibleRows(STORE, context({ query: 'nothing-like-this' }))).toEqual([])
  })
})

describe('filters', () => {
  const states: Record<string, Checked> = { '/guide/': 'mixed', '/blog/': 'false', '/changelog': 'true', '/guide/install': 'true', '/guide/quickstart': 'false' }
  const stateOf = (item: TreeNodeDto) => states[item.prefix] ?? 'true'

  it('keeps the ticked and the partly ticked rows for the ticked filter', () => {
    const open = new Set(['/guide/'])

    expect(prefixes(visibleRows(STORE, context({ filter: 'ticked', stateOf, open })))).toEqual(['0:/guide/', '1:/guide/install', '0:/changelog'])
  })

  it('keeps the unticked and the partly ticked rows for the unticked filter', () => {
    const open = new Set(['/guide/'])

    expect(prefixes(visibleRows(STORE, context({ filter: 'unticked', stateOf, open })))).toEqual(['0:/guide/', '1:/guide/quickstart', '0:/blog/'])
  })
})

describe('labelOf', () => {
  it('names pages, folders, the home page and the rest of a folder', () => {
    expect(labelOf(node('/guide/'))).toBe('/guide/')
    expect(labelOf(node('/', 'Page'))).toBe('/ (home page)')
    expect(labelOf(node('/guide/', 'Page'))).toBe('/guide/ (this page)')
    expect(labelOf(node('/guide/install', 'Page'))).toBe('/guide/install')
    expect(labelOf(node('/guide/', 'Rest'))).toBe('Other pages in /guide/')
  })
})
