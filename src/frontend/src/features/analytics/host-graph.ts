import type { HostLinkEdge } from '@/types/api'

export type GraphNode = {
  host: string
  /** Links that leave this host and links that arrive at it. */
  out: number
  in: number
  x: number
  y: number
  radius: number
}

export type GraphEdge = HostLinkEdge & { source: GraphNode; target: GraphNode; width: number }

export type HostGraph = { nodes: GraphNode[]; edges: GraphEdge[]; width: number; height: number }

export const GRAPH_WIDTH = 720
export const GRAPH_HEIGHT = 480
const MARGIN = 44
const ITERATIONS = 220
const MIN_RADIUS = 6
const MAX_RADIUS = 22

/** Maps v in [0, max] onto [lo, hi] by square root, so one huge host does not shrink the rest to dots. */
function scale(v: number, max: number, lo: number, hi: number): number {
  return max <= 0 ? lo : lo + (hi - lo) * Math.sqrt(v / max)
}

const clamp = (v: number, lo: number, hi: number) => Math.min(Math.max(v, lo), hi)

/** A host is a node when any edge touches it. Sorted by name so the result does not depend on edge order. */
function collectNodes(edges: HostLinkEdge[]): Map<string, GraphNode> {
  const byHost = new Map<string, GraphNode>()
  const node = (host: string): GraphNode => {
    const found = byHost.get(host) ?? { host, out: 0, in: 0, x: 0, y: 0, radius: MIN_RADIUS }
    byHost.set(host, found)
    return found
  }
  for (const e of edges) {
    node(e.fromHost).out += e.links
    node(e.toHost).in += e.links
  }
  return new Map([...byHost].sort(([a], [b]) => a.localeCompare(b)))
}

/** Puts the nodes on a circle and sizes them by their links. */
function place(nodes: GraphNode[], width: number, height: number): void {
  const maxDegree = Math.max(0, ...nodes.map((n) => n.in + n.out))
  nodes.forEach((n, i) => {
    const angle = (2 * Math.PI * i) / nodes.length
    n.x = width / 2 + (Math.cos(angle) * (width - 2 * MARGIN)) / 3
    n.y = height / 2 + (Math.sin(angle) * (height - 2 * MARGIN)) / 3
    n.radius = scale(n.in + n.out, maxDegree, MIN_RADIUS, MAX_RADIUS)
  })
}

/**
 * Builds the nodes and edges and lays them out with a Fruchterman-Reingold force simulation. It starts on a circle
 * and uses no randomness, so the same edges always give the same picture.
 */
export function buildHostGraph(edges: HostLinkEdge[], width = GRAPH_WIDTH, height = GRAPH_HEIGHT): HostGraph {
  const byHost = collectNodes(edges)
  const nodes = [...byHost.values()]
  const maxLinks = Math.max(0, ...edges.map((e) => e.links))
  place(nodes, width, height)
  const graphEdges: GraphEdge[] = edges.map((e) => ({
    ...e,
    source: byHost.get(e.fromHost)!,
    target: byHost.get(e.toHost)!,
    width: scale(e.links, maxLinks, 1, 5),
  }))
  layout(nodes, graphEdges, width, height)
  return { nodes, edges: graphEdges, width, height }
}

type Forces = { dx: number[]; dy: number[] }

/** Every pair of nodes pushes apart. */
function repel(nodes: GraphNode[], f: Forces, k: number): void {
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) {
      // Two nodes on the same spot get a tiny fixed nudge instead of a random one.
      const same = nodes[i].x === nodes[j].x && nodes[i].y === nodes[j].y
      const vx = same ? 0.01 * (i - j) : nodes[i].x - nodes[j].x
      const vy = same ? 0.01 : nodes[i].y - nodes[j].y
      const push = (k * k) / Math.hypot(vx, vy) ** 2
      f.dx[i] += vx * push
      f.dy[i] += vy * push
      f.dx[j] -= vx * push
      f.dy[j] -= vy * push
    }
  }
}

/** Every edge pulls its two ends together. */
function attract(edges: GraphEdge[], index: Map<GraphNode, number>, f: Forces, k: number): void {
  for (const e of edges) {
    const i = index.get(e.source)!
    const j = index.get(e.target)!
    const vx = e.source.x - e.target.x
    const vy = e.source.y - e.target.y
    const pull = (Math.hypot(vx, vy) || 0.01) / k
    f.dx[i] -= vx * pull
    f.dy[i] -= vy * pull
    f.dx[j] += vx * pull
    f.dy[j] += vy * pull
  }
}

/** Moves each node along its force, by at most the temperature, and keeps it on the canvas. */
function move(nodes: GraphNode[], f: Forces, temperature: number, width: number, height: number): void {
  nodes.forEach((n, i) => {
    // A gentle pull to the middle keeps disconnected clusters on the canvas.
    const fx = f.dx[i] + (width / 2 - n.x) * 0.02
    const fy = f.dy[i] + (height / 2 - n.y) * 0.02
    const len = Math.hypot(fx, fy) || 1
    const step = Math.min(len, temperature)
    n.x = clamp(n.x + (fx / len) * step, MARGIN, width - MARGIN)
    n.y = clamp(n.y + (fy / len) * step, MARGIN, height - MARGIN)
  })
}

function layout(nodes: GraphNode[], edges: GraphEdge[], width: number, height: number): void {
  if (nodes.length < 2) {
    nodes.forEach((n) => Object.assign(n, { x: width / 2, y: height / 2 }))
    return
  }
  const k = Math.sqrt(((width - 2 * MARGIN) * (height - 2 * MARGIN)) / nodes.length) * 0.8
  const index = new Map(nodes.map((n, i) => [n, i]))
  const cooling = width / 8 / (ITERATIONS + 1)
  for (let step = 0; step < ITERATIONS; step++) {
    const forces: Forces = { dx: nodes.map(() => 0), dy: nodes.map(() => 0) }
    repel(nodes, forces, k)
    attract(edges, index, forces, k)
    move(nodes, forces, width / 8 - cooling * step, width, height)
  }
}

/** Hosts that share an edge with `host`, plus the host itself. */
export function neighbours(graph: HostGraph, host: string): Set<string> {
  const set = new Set([host])
  for (const e of graph.edges) {
    if (e.fromHost === host) {
      set.add(e.toHost)
    } else if (e.toHost === host) {
      set.add(e.fromHost)
    }
  }
  return set
}
