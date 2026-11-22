import type { PathRule, TreeNodeDto } from '@/types/api'

export type Checked = 'true' | 'false' | 'mixed'

export type NodeRef = Pick<TreeNodeDto, 'prefix' | 'kind'>

export const CLEARED: PathRule[] = [{ kind: 'Exclude', prefix: '/' }]

export function pathOf(url: string): string {
  try {
    return new URL(url).pathname
  } catch {
    return '/'
  }
}

export function withPin(rules: PathRule[], startPath: string): PathRule[] {
  return [...rules, { kind: 'Include', prefix: startPath, exact: true }]
}

function covers(rule: PathRule, path: string): boolean {
  if (rule.exact) {
    return path === rule.prefix
  }
  const folder = path.endsWith('/') ? path : `${path}/`
  return folder.startsWith(rule.prefix)
}

function strength(rule: PathRule): [number, number, number] {
  return [rule.exact ? 1 : 0, rule.prefix.length, rule.kind === 'Include' ? 1 : 0]
}

function stronger(left: PathRule, right: PathRule): PathRule {
  const a = strength(left)
  const b = strength(right)
  const index = a.findIndex((value, at) => value !== b[at])
  return index >= 0 && a[index] < b[index] ? right : left
}

export function decide(rules: PathRule[], path: string): boolean {
  const matching = rules.filter((rule) => covers(rule, path))
  return matching.length === 0 || matching.reduce(stronger).kind === 'Include'
}

function isInside(rule: PathRule, folder: string): boolean {
  return rule.prefix.startsWith(folder) && !(rule.prefix === folder && !rule.exact)
}

export function checkState(rules: PathRule[], node: NodeRef): Checked {
  const own = decide(rules, node.prefix)
  if (node.kind !== 'Folder') {
    return own ? 'true' : 'false'
  }
  const deviates = rules.some((rule) => isInside(rule, node.prefix) && decide(rules, rule.prefix) !== own)
  if (deviates) {
    return 'mixed'
  }
  return own ? 'true' : 'false'
}

function isOwnRule(rule: PathRule, node: NodeRef): boolean {
  return rule.prefix === node.prefix && Boolean(rule.exact) === (node.kind === 'Page')
}

export function setChecked(rules: PathRule[], node: NodeRef, checked: boolean, startPath: string): PathRule[] {
  const kept = rules.filter((rule) => !isOwnRule(rule, node) && !(node.kind === 'Folder' && isInside(rule, node.prefix)))
  if (decide(withPin(kept, startPath), node.prefix) === checked) {
    return kept
  }
  return [...kept, { kind: checked ? 'Include' : 'Exclude', prefix: node.prefix, exact: node.kind === 'Page' ? true : undefined }]
}

export function nextChecked(state: Checked): boolean {
  return state !== 'true'
}
