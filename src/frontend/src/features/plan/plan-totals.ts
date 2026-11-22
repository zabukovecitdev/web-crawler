import type { PathRule, TreeNodeDto } from '@/types/api'
import { decide } from './path-rules'
import type { TreeStore } from './tree-store'

type Weight = (node: TreeNodeDto) => number

function sumOf(store: TreeStore, node: TreeNodeDto, weight: Weight): number {
  if (node.blocked) {
    return 0
  }
  const children = node.kind === 'Folder' ? store[node.prefix] : undefined
  return children ? children.reduce((sum, child) => sum + sumOf(store, child, weight), 0) : weight(node)
}

function total(store: TreeStore, weight: Weight): number {
  return (store['/'] ?? []).reduce((sum, node) => sum + sumOf(store, node, weight), 0)
}

export function totalPages(store: TreeStore): number {
  return total(store, (node) => node.counts.pages)
}

export function tickedPages(store: TreeStore, rules: PathRule[]): number {
  return total(store, (node) => (decide(rules, node.prefix) ? node.counts.pages : 0))
}

export function tickedIn(store: TreeStore, node: TreeNodeDto, rules: PathRule[]): number {
  return sumOf(store, node, (leaf) => (decide(rules, leaf.prefix) ? leaf.counts.pages : 0))
}

export function totalIn(store: TreeStore, node: TreeNodeDto): number {
  return sumOf(store, node, (leaf) => leaf.counts.pages)
}
