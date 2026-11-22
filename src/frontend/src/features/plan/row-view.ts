import type { PathRule, TreeNodeDto } from '@/types/api'
import { formatNumber } from './plan-summary'
import { tickedIn, totalIn } from './plan-totals'
import { labelOf, type RowModel } from './tree-rows'
import type { TreeStore } from './tree-store'
import { formatRange } from './format-range'
import { processedOf, runStatus, type JobContext, type RowStatus } from './run-status'

export type RowView = {
  key: string
  level: number
  label: string
  state: RowModel['state']
  expandable: boolean
  open: boolean
  disabled: boolean
  chips: string[]
  note: string
  count: string
  doneShare: number
  failedShare: number
  updated: string
  status: RowStatus | null
  node: TreeNodeDto
}

const BLOCKED_NOTE = 'robots.txt disallows this for SamoBot. It is never requested.'

function share(part: number, whole: number): number {
  return whole > 0 ? Math.max(0, Math.min(100, (part / whole) * 100)) : 0
}

function baseOf(row: RowModel, updated: string): RowView {
  const { node } = row
  return {
    key: `${row.parent}|${node.kind}|${node.prefix}`,
    level: row.level,
    label: labelOf(node),
    state: node.blocked ? 'false' : row.state,
    expandable: node.kind === 'Folder',
    open: row.open,
    disabled: node.blocked || node.kind === 'Rest',
    chips: [],
    note: node.blocked ? BLOCKED_NOTE : '',
    count: formatNumber(node.counts.pages),
    doneShare: 0,
    failedShare: 0,
    updated,
    status: null,
    node,
  }
}

export type PlanContext = { store: TreeStore; rules: PathRule[]; startPath: string; hasSitemap: boolean }

function planStatus(view: RowView, isStart: boolean): RowStatus | null {
  if (view.node.blocked) {
    return { text: 'Blocked by robots.txt', tone: 'mute', live: false }
  }
  if (isStart) {
    return { text: 'Fetched first', tone: 'info', live: false }
  }
  if (view.state === 'false') {
    return { text: 'Not fetched', tone: 'mute', live: false }
  }
  return view.state === 'mixed' ? { text: 'Partly selected', tone: 'info', live: false } : null
}

function countText(total: number, ticked: number): string {
  return ticked === total ? formatNumber(total) : `${formatNumber(ticked)} of ${formatNumber(total)}`
}

export function planRowView(row: RowModel, ctx: PlanContext): RowView {
  const { node } = row
  const view = baseOf(row, ctx.hasSitemap ? formatRange(node.counts.oldestModified, node.counts.newestModified) : '')
  const isStart = node.kind === 'Page' && node.prefix === ctx.startPath
  const ticked = tickedIn(ctx.store, node, ctx.rules)
  const total = totalIn(ctx.store, node)
  return {
    ...view,
    disabled: view.disabled || isStart,
    chips: isStart ? ['Your start page'] : [],
    note: isStart ? 'Your start page is always fetched.' : view.note,
    count: countText(node.blocked ? node.counts.pages : total, node.blocked ? node.counts.pages : ticked),
    doneShare: share(ticked, total),
    status: planStatus(view, isStart),
  }
}

export type RunContext = { job: JobContext; startPath: string; inBrowser: boolean; editable: boolean }

function runNote(view: RowView, processed: number): string {
  if (view.node.blocked) {
    return BLOCKED_NOTE
  }
  return view.state === 'false' && processed > 0 ? `${formatNumber(processed)} ${processed === 1 ? 'page' : 'pages'} fetched before you unticked it. They are kept.` : ''
}

function chipsOf(isStart: boolean, inBrowser: boolean, processed: number): string[] {
  return [...(isStart ? ['Your start page'] : []), ...(inBrowser && processed > 0 ? ['JS'] : [])]
}

export function runRowView(row: RowModel, ctx: RunContext): RowView {
  const { node } = row
  const counts = node.counts
  const view = baseOf(row, '')
  const isStart = node.kind === 'Page' && node.prefix === ctx.startPath
  const processed = processedOf(counts)
  const wanted = view.state === 'false' ? processed : counts.pages - counts.skipped - counts.stopped
  return {
    ...view,
    disabled: view.disabled || !ctx.editable || isStart,
    chips: chipsOf(isStart, ctx.inBrowser, processed),
    note: runNote(view, processed),
    count: node.blocked ? formatNumber(counts.pages) : `${formatNumber(processed)} / ${formatNumber(wanted)}`,
    doneShare: share(counts.done, wanted),
    failedShare: share(counts.failed, wanted),
    status: runStatus(counts, view.state, node.blocked, ctx.job),
  }
}
