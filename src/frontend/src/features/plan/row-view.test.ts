import { describe, expect, it } from 'vitest'
import type { PathRule, TreeNodeDto } from '@/types/api'
import { planRowView, runRowView } from './row-view'
import type { RowModel } from './tree-rows'
import { emptyCounts, type TreeStore } from './tree-store'

const node = (prefix: string, pages: number, patch: Partial<TreeNodeDto> = {}): TreeNodeDto => ({
  prefix,
  kind: prefix.endsWith('/') ? 'Folder' : 'Page',
  hasChildren: prefix.endsWith('/'),
  blocked: false,
  counts: { ...emptyCounts(), pages },
  children: null,
  ...patch,
})

const rowOf = (item: TreeNodeDto, state: RowModel['state'] = 'true'): RowModel => ({ node: item, parent: '/', level: 0, state, open: false })

const blog = node('/blog/', 300)
const STORE: TreeStore = { '/': [blog, node('/guide/install', 1)], '/blog/': [node('/blog/2016/', 100), node('/blog/2026/', 200)] }

function plan(item: TreeNodeDto, state: RowModel['state'], rules: PathRule[] = []) {
  return planRowView(rowOf(item, state), { store: STORE, rules, startPath: '/guide/install', hasSitemap: true })
}

describe('planRowView', () => {
  it('shows a plain ticked folder with its count and no status', () => {
    const view = plan(blog, 'true')

    expect([view.count, view.doneShare, view.status, view.disabled]).toEqual(['300', 100, null, false])
  })

  it('shows how much of a partly ticked folder is ticked', () => {
    const view = plan(blog, 'mixed', [{ kind: 'Exclude', prefix: '/blog/2016/' }])

    expect([view.count, view.status?.text]).toEqual(['200 of 300', 'Partly selected'])
  })

  it('shows an unticked folder as not fetched', () => {
    expect(plan(blog, 'false', [{ kind: 'Exclude', prefix: '/blog/' }]).status?.text).toBe('Not fetched')
  })

  it('locks the start page', () => {
    const view = plan(node('/guide/install', 1), 'true')

    expect([view.disabled, view.chips, view.status?.text, view.note]).toEqual([true, ['Your start page'], 'Fetched first', 'Your start page is always fetched.'])
  })

  it('shows a blocked folder as unticked and locked', () => {
    const view = plan(node('/admin/', 50, { blocked: true }), 'true')

    expect([view.state, view.disabled, view.status?.text]).toEqual(['false', true, 'Blocked by robots.txt'])
  })
})

describe('runRowView', () => {
  const run = { job: { status: 'Running' as const, pauseReason: null }, startPath: '/guide/install', inBrowser: true, editable: true }
  const folder = node('/blog/', 100, { counts: { ...emptyCounts(), pages: 100, done: 40, failed: 10, queued: 50 } })

  it('shows processed of wanted with the shares of done and failed', () => {
    const view = runRowView(rowOf(folder), run)

    expect([view.count, view.doneShare, view.failedShare, view.chips, view.status?.text]).toEqual(['50 / 100', 40, 10, ['JS'], 'In progress'])
  })

  it('keeps the pages fetched before the folder was unticked', () => {
    const view = runRowView(rowOf(folder, 'false'), run)

    expect([view.count, view.note, view.status?.text]).toEqual(['50 / 50', '50 pages fetched before you unticked it. They are kept.', 'Stopped'])
  })

  it('cannot be changed when the job is over', () => {
    expect(runRowView(rowOf(folder), { ...run, editable: false }).disabled).toBe(true)
  })
})
