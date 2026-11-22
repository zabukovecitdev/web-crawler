import { describe, expect, it } from 'vitest'
import type { PathTreeDto, TreeNodeDto } from '@/types/api'
import { buildStore, emptyCounts, folderPrefixes, providedParents, totalOf } from './tree-store'

const node = (prefix: string, pages: number, children: TreeNodeDto[] | null = null): TreeNodeDto => ({
  prefix,
  kind: prefix.endsWith('/') ? 'Folder' : 'Page',
  hasChildren: prefix.endsWith('/'),
  blocked: false,
  counts: { ...emptyCounts(), pages },
  children,
})

const tree = (parent: string, nodes: TreeNodeDto[]): PathTreeDto => ({ host: 'a.example', parent, total: emptyCounts(), nodes })

describe('buildStore', () => {
  it('stores the children of the parent and of every folder that came with children', () => {
    const level = { data: tree('/', [node('/a/', 3, [node('/a/x', 1), node('/a/y/', 2)]), node('/b', 1)]), updatedAt: 1 }

    const store = buildStore([level])

    expect(Object.keys(store).sort()).toEqual(['/', '/a/'])
    expect(store['/a/'].map((entry) => entry.prefix)).toEqual(['/a/x', '/a/y/'])
    expect(store['/'][0].children).toBeNull()
  })

  it('lets the newer answer win for the same folder', () => {
    const older = { data: tree('/', [node('/a/', 3, [node('/a/x', 1)])]), updatedAt: 1 }
    const newer = { data: tree('/a/', [node('/a/x', 1), node('/a/z', 5)]), updatedAt: 2 }

    expect(buildStore([newer, older])['/a/'].map((entry) => entry.prefix)).toEqual(['/a/x', '/a/z'])
  })

  it('knows which folders already have their children', () => {
    const level = { data: tree('/', [node('/a/', 3, [node('/a/x', 1)]), node('/b/', 1)]), updatedAt: 1 }

    expect([...providedParents([level])].sort()).toEqual(['/', '/a/'])
  })

  it('lists the folders it holds and the total of the first level', () => {
    const store = buildStore([{ data: tree('/', [node('/a/', 3), node('/b', 4), node('/c/', 5)]), updatedAt: 1 }])

    expect(folderPrefixes(store)).toEqual(['/a/', '/c/'])
    expect(totalOf(store).pages).toBe(12)
  })
})
