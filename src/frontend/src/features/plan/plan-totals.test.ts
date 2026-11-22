import { describe, expect, it } from 'vitest'
import type { PathRule, TreeNodeDto } from '@/types/api'
import { tickedPages, totalPages } from './plan-totals'
import { emptyCounts, type TreeStore } from './tree-store'

const node = (prefix: string, pages: number, kind: TreeNodeDto['kind'] = 'Folder', blocked = false): TreeNodeDto => ({
  prefix,
  kind,
  hasChildren: kind === 'Folder',
  blocked,
  counts: { ...emptyCounts(), pages },
  children: null,
})

const STORE: TreeStore = {
  '/': [node('/guide/', 3), node('/blog/', 300), node('/admin/', 50, 'Folder', true), node('/changelog', 37, 'Page')],
  '/guide/': [node('/guide/install', 1, 'Page'), node('/guide/', 1, 'Page'), node('/guide/', 1, 'Rest')],
  '/blog/': [node('/blog/2016/', 100), node('/blog/2026/', 200)],
}

const exclude = (prefix: string): PathRule => ({ kind: 'Exclude', prefix })

describe('totals', () => {
  it('sums every top-level node but never the blocked ones', () => {
    expect(totalPages(STORE)).toBe(3 + 300 + 37)
  })

  it('ticks everything when there are no rules', () => {
    expect(tickedPages(STORE, [])).toBe(340)
  })

  it('subtracts an unticked folder and a page', () => {
    expect(tickedPages(STORE, [exclude('/blog/'), exclude('/changelog')])).toBe(3)
  })

  it('subtracts the part of a folder that is unticked below its loaded children', () => {
    expect(tickedPages(STORE, [exclude('/blog/2016/')])).toBe(240)
  })

  it('adds a ticked folder inside an unticked one', () => {
    const rules: PathRule[] = [exclude('/blog/'), { kind: 'Include', prefix: '/blog/2026/' }]
    expect(tickedPages(STORE, rules)).toBe(3 + 200 + 37)
  })

  it('lets the rest of a folder follow the folder', () => {
    expect(tickedPages(STORE, [exclude('/guide/')])).toBe(300 + 37)
  })

  it('counts an unloaded folder as one block', () => {
    expect(tickedPages({ '/': [node('/a/', 10)] }, [])).toBe(10)
    expect(tickedPages({ '/': [node('/a/', 10)] }, [exclude('/a/')])).toBe(0)
  })

  it('is empty without data', () => {
    expect([totalPages({}), tickedPages({}, [])]).toEqual([0, 0])
  })
})
