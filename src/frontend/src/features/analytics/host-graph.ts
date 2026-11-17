import type { HostLinkEdge } from '@/types/api'

/** A site. `val` is the area weight the drawing scales the circle by. */
export type GraphNode = { id: string; out: number; in: number; val: number }

/** A connection. source and target are host names until the library swaps them for node objects. */
export type GraphLink = HostLinkEdge & { source: string; target: string; width: number }

export type HostGraph = { nodes: GraphNode[]; links: GraphLink[] }

const MIN_WIDTH = 1
const MAX_WIDTH = 5

/** Maps v in [0, max] onto [lo, hi] by square root, so one huge link count does not shrink the rest to hairlines. */
function scale(v: number, max: number, lo: number, hi: number): number {
  return max <= 0 ? lo : lo + (hi - lo) * Math.sqrt(v / max)
}

/** A host is a node when any edge touches it. Sorted by name so the result does not depend on edge order. */
function collectNodes(edges: HostLinkEdge[]): GraphNode[] {
  const byHost = new Map<string, GraphNode>()
  const node = (id: string): GraphNode => {
    const found = byHost.get(id) ?? { id, out: 0, in: 0, val: 1 }
    byHost.set(id, found)
    return found
  }
  for (const e of edges) {
    node(e.fromHost).out += e.links
    node(e.toHost).in += e.links
  }
  const nodes = [...byHost.values()].sort((a, b) => a.id.localeCompare(b.id))
  for (const n of nodes) {
    n.val = n.in + n.out
  }
  return nodes
}

export function buildHostGraph(edges: HostLinkEdge[]): HostGraph {
  const maxLinks = Math.max(0, ...edges.map((e) => e.links))
  return {
    nodes: collectNodes(edges),
    links: edges.map((e) => ({
      ...e,
      source: e.fromHost,
      target: e.toHost,
      width: scale(e.links, maxLinks, MIN_WIDTH, MAX_WIDTH),
    })),
  }
}

/** Hosts that share a connection with `host`, plus the host itself. */
export function neighbours(graph: HostGraph, host: string): Set<string> {
  const set = new Set([host])
  for (const e of graph.links) {
    if (e.fromHost === host) {
      set.add(e.toHost)
    } else if (e.toHost === host) {
      set.add(e.fromHost)
    }
  }
  return set
}

/** The ids of the busiest `limit` sites: the ones that always get a label. */
export function busiest(graph: HostGraph, limit: number): Set<string> {
  const ranked = [...graph.nodes].sort((a, b) => b.val - a.val)
  return new Set(ranked.slice(0, limit).map((n) => n.id))
}
