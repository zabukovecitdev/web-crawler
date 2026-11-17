import { describe, expect, it } from 'vitest'
import type { HostLinkEdge } from '@/types/api'
import { buildHostGraph, busiest, neighbours } from './host-graph'

const edge = (fromHost: string, toHost: string, links: number, pages = 1): HostLinkEdge => ({
  fromHost,
  toHost,
  links,
  pages,
})

const EDGES = [edge('a.example', 'b.example', 30), edge('a.example', 'c.example', 5), edge('b.example', 'c.example', 1)]

describe('buildHostGraph', () => {
  it('makes one node per host, sorted, and counts links in and out', () => {
    const { nodes, links } = buildHostGraph(EDGES)
    const byId = Object.fromEntries(nodes.map((n) => [n.id, n]))

    expect(nodes.map((n) => n.id)).toEqual(['a.example', 'b.example', 'c.example'])
    expect(byId['a.example']).toMatchObject({ out: 35, in: 0, val: 35 })
    expect(byId['b.example']).toMatchObject({ out: 1, in: 30, val: 31 })
    expect(byId['c.example']).toMatchObject({ out: 0, in: 6, val: 6 })
    expect(links).toHaveLength(3)
  })

  it('points each link from the linking host to the linked host', () => {
    const [first] = buildHostGraph(EDGES).links

    expect(first).toMatchObject({ source: 'a.example', target: 'b.example', links: 30 })
  })

  it('draws strong connections wider, within 1 to 5', () => {
    const { links } = buildHostGraph(EDGES)

    expect(links[0].width).toBe(5)
    expect(links[0].width).toBeGreaterThan(links[2].width)
    expect(links[2].width).toBeGreaterThanOrEqual(1)
  })

  it('handles no edges', () => {
    expect(buildHostGraph([])).toEqual({ nodes: [], links: [] })
  })
})

describe('neighbours and busiest', () => {
  it('neighbours returns the host and the hosts it is linked with in either direction', () => {
    const graph = buildHostGraph([...EDGES, edge('d.example', 'e.example', 1)])

    expect([...neighbours(graph, 'c.example')].sort()).toEqual(['a.example', 'b.example', 'c.example'])
    expect([...neighbours(graph, 'd.example')].sort()).toEqual(['d.example', 'e.example'])
  })

  it('busiest returns the sites with the most links in plus out', () => {
    expect([...busiest(buildHostGraph(EDGES), 2)].sort()).toEqual(['a.example', 'b.example'])
  })
})
