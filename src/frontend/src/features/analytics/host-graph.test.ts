import { describe, expect, it } from 'vitest'
import type { HostLinkEdge } from '@/types/api'
import { buildHostGraph, GRAPH_HEIGHT, GRAPH_WIDTH, neighbours } from './host-graph'

const edge = (fromHost: string, toHost: string, links: number, pages = 1): HostLinkEdge => ({
  fromHost,
  toHost,
  links,
  pages,
})

const EDGES = [edge('a.example', 'b.example', 30), edge('a.example', 'c.example', 5), edge('b.example', 'c.example', 1)]

describe('buildHostGraph nodes', () => {
  it('makes one node per host and counts links in and out', () => {
    const graph = buildHostGraph(EDGES)
    const byHost = Object.fromEntries(graph.nodes.map((n) => [n.host, n]))

    expect(graph.nodes.map((n) => n.host)).toEqual(['a.example', 'b.example', 'c.example'])
    expect(byHost['a.example']).toMatchObject({ out: 35, in: 0 })
    expect(byHost['b.example']).toMatchObject({ out: 1, in: 30 })
    expect(byHost['c.example']).toMatchObject({ out: 0, in: 6 })
    expect(graph.edges).toHaveLength(3)
  })

})

describe('buildHostGraph layout', () => {
  it('keeps every node inside the canvas', () => {
    const { nodes } = buildHostGraph(EDGES)

    for (const n of nodes) {
      expect(n.x).toBeGreaterThanOrEqual(0)
      expect(n.x).toBeLessThanOrEqual(GRAPH_WIDTH)
      expect(n.y).toBeGreaterThanOrEqual(0)
      expect(n.y).toBeLessThanOrEqual(GRAPH_HEIGHT)
    }
  })

  it('gives the same picture for the same edges', () => {
    const a = buildHostGraph(EDGES).nodes.map((n) => [n.x, n.y])
    const b = buildHostGraph(EDGES).nodes.map((n) => [n.x, n.y])

    expect(a).toEqual(b)
  })

  it('sizes busy hosts and strong edges larger', () => {
    const graph = buildHostGraph(EDGES)
    const radius = (h: string) => graph.nodes.find((n) => n.host === h)!.radius

    expect(radius('b.example')).toBeGreaterThan(radius('c.example'))
    expect(graph.edges[0].width).toBeGreaterThan(graph.edges[2].width)
  })

  it('handles no edges and a single edge', () => {
    expect(buildHostGraph([])).toMatchObject({ nodes: [], edges: [] })
    const one = buildHostGraph([edge('a.example', 'b.example', 1)])
    expect(one.nodes).toHaveLength(2)
    expect(one.nodes.every((n) => Number.isFinite(n.x) && Number.isFinite(n.y))).toBe(true)
  })
})

describe('neighbours', () => {
  it('returns the host and the hosts it is linked with in either direction', () => {
    const graph = buildHostGraph(EDGES)

    expect([...neighbours(graph, 'c.example')].sort()).toEqual(['a.example', 'b.example', 'c.example'])
    expect([...neighbours(graph, 'a.example')].sort()).toEqual(['a.example', 'b.example', 'c.example'])
  })
})
