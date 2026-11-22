import type { TreeNodeDto } from '@/types/api'
import type { Checked } from './path-rules'
import type { TreeStore } from './tree-store'

export type TreeFilter = 'all' | 'ticked' | 'unticked'

export type RowModel = { node: TreeNodeDto; parent: string; level: number; state: Checked; open: boolean }

export type RowContext = {
  open: ReadonlySet<string>
  query: string
  filter: TreeFilter
  stateOf: (node: TreeNodeDto) => Checked
}

function isHit(node: TreeNodeDto, query: string): boolean {
  return query === '' || node.prefix.toLowerCase().includes(query)
}

function hasHitBelow(store: TreeStore, node: TreeNodeDto, query: string): boolean {
  const children = node.kind === 'Folder' ? store[node.prefix] : undefined
  return (children ?? []).some((child) => isHit(child, query) || hasHitBelow(store, child, query))
}

function passes(state: Checked, filter: TreeFilter): boolean {
  if (filter === 'all' || state === 'mixed') {
    return true
  }
  return (filter === 'ticked') === (state === 'true')
}

function shows(store: TreeStore, node: TreeNodeDto, state: Checked, forced: boolean, ctx: RowContext): boolean {
  return (isHit(node, ctx.query) || forced || hasHitBelow(store, node, ctx.query)) && passes(state, ctx.filter)
}

function isOpen(node: TreeNodeDto, ctx: RowContext): boolean {
  return node.kind === 'Folder' && (ctx.open.has(node.prefix) || ctx.query !== '')
}

function forcesChildren(node: TreeNodeDto, forced: boolean, ctx: RowContext): boolean {
  return forced || (ctx.query !== '' && isHit(node, ctx.query))
}

function walk(store: TreeStore, parent: string, level: number, ctx: RowContext, forced: boolean, out: RowModel[]): void {
  for (const node of store[parent] ?? []) {
    const state = ctx.stateOf(node)
    if (!shows(store, node, state, forced, ctx)) {
      continue
    }
    const open = isOpen(node, ctx)
    out.push({ node, parent, level, state, open })
    if (open) {
      walk(store, node.prefix, level + 1, ctx, forcesChildren(node, forced, ctx), out)
    }
  }
}

export function visibleRows(store: TreeStore, ctx: RowContext): RowModel[] {
  const rows: RowModel[] = []
  walk(store, '/', 0, { ...ctx, query: ctx.query.trim().toLowerCase() }, false, rows)
  return rows
}

export function labelOf(node: TreeNodeDto): string {
  if (node.kind === 'Rest') {
    return `Other pages in ${node.prefix}`
  }
  if (node.prefix === '/') {
    return '/ (home page)'
  }
  return node.kind === 'Page' && node.prefix.endsWith('/') ? `${node.prefix} (this page)` : node.prefix
}
