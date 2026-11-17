import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ForceGraph2D, { type ForceGraphMethods } from 'react-force-graph-2d'
import { formatNumber } from '@/lib/format'
import { type Node, paintHitArea, paintNode } from './graph-paint'
import { useElementWidth, useGraphPalette } from './graph-palette'
import { busiest, type GraphLink, type GraphNode, type HostGraph, neighbours } from './host-graph'

type Props = { graph: HostGraph; focus: string | undefined; onFocus: (host: string | undefined) => void }

const HEIGHT = 480
const LABEL_LIMIT = 20
const MIN_RADIUS = 5
const MAX_RADIUS = 20

/** The host name of a link end: a name before the simulation starts, a node object after. */
const endId = (end: GraphLink['source'] | Node): string => (typeof end === 'object' ? String(end.id) : end)

/** What the hovered or filtered site highlights, and how big each circle is. */
function useHighlight(graph: HostGraph, active: string | undefined) {
  const near = useMemo(() => (active ? neighbours(graph, active) : undefined), [graph, active])
  const labelled = useMemo(() => busiest(graph, LABEL_LIMIT), [graph])
  const maxVal = Math.max(1, ...graph.nodes.map((n) => n.val))
  const radius = useCallback((n: GraphNode) => MIN_RADIUS + (MAX_RADIUS - MIN_RADIUS) * Math.sqrt(n.val / maxVal), [maxVal])
  return { near, labelled, radius }
}

/** Sites repel each other and connections hold them at a readable distance; the defaults are too weak for big circles. */
function useSpacing(fg: React.RefObject<ForceGraphMethods<GraphNode, GraphLink> | undefined>, data: object, ready: boolean) {
  useEffect(() => {
    const graph = fg.current
    if (!ready || !graph) {
      return
    }
    graph.d3Force('charge')?.strength(-400)
    graph.d3Force('link')?.distance(150)
    graph.d3ReheatSimulation()
  }, [fg, data, ready])
}

/** The graph on a canvas: physics layout, drag a site, scroll to zoom, drag the background to pan. */
export function HostGraphCanvas({ graph, focus, onFocus }: Props) {
  const box = useRef<HTMLDivElement>(null)
  const fg = useRef<ForceGraphMethods<GraphNode, GraphLink> | undefined>(undefined)
  const width = useElementWidth(box)
  const palette = useGraphPalette()
  const [hover, setHover] = useState<string | undefined>()
  const active = hover ?? focus
  const { near, labelled, radius } = useHighlight(graph, active)
  // The library rewrites nodes and links in place, so it gets copies.
  const data = useMemo(() => ({ nodes: graph.nodes.map((n) => ({ ...n })), links: graph.links.map((l) => ({ ...l })) }), [graph])

  const paint = useCallback(
    (node: Node, ctx: CanvasRenderingContext2D, zoom: number) =>
      paintNode(node, ctx, zoom, {
        radius: radius(node as GraphNode),
        palette,
        dim: near !== undefined && !near.has(String(node.id)),
        selected: node.id === focus,
        labelled: labelled.has(String(node.id)) || node.id === active,
      }),
    [radius, near, focus, active, labelled, palette],
  )
  const touches = useCallback(
    (l: GraphLink) => !active || endId(l.source) === active || endId(l.target) === active,
    [active],
  )
  const linkColour = useCallback((l: GraphLink) => (touches(l) ? palette.line : palette.track), [touches, palette])
  const arrowColour = useCallback((l: GraphLink) => (touches(l) ? palette.faint : palette.track), [touches, palette])

  useSpacing(fg, data, width > 0)

  return (
    <div ref={box} className="overflow-hidden rounded-md bg-muted/40" style={{ height: HEIGHT }}>
      {width > 0 ? (
        <ForceGraph2D<GraphNode, GraphLink>
          ref={fg}
          graphData={data}
          width={width}
          height={HEIGHT}
          nodeRelSize={1}
          nodeVal={(n) => radius(n as GraphNode) ** 2}
          backgroundColor="rgba(0,0,0,0)"
          nodeCanvasObject={paint}
          nodeCanvasObjectMode={() => 'replace'}
          nodePointerAreaPaint={(n, colour, ctx) => paintHitArea(n, colour, ctx, radius(n as GraphNode))}
          nodeLabel={(n) => `${n.id}: ${formatNumber(n.out)} links out, ${formatNumber(n.in)} in`}
          linkLabel={(l) => `${l.fromHost} → ${l.toHost}: ${formatNumber(l.links)} links on ${formatNumber(l.pages)} pages`}
          linkColor={linkColour}
          linkWidth={(l) => l.width}
          linkCurvature={0.15}
          linkDirectionalArrowLength={(l) => 7 + l.width * 1.5}
          linkDirectionalArrowRelPos={1}
          linkDirectionalArrowColor={arrowColour}
          onNodeHover={(n) => setHover(n ? String(n.id) : undefined)}
          onNodeClick={(n) => onFocus(n.id === focus ? undefined : String(n.id))}
          onBackgroundClick={() => onFocus(undefined)}
          cooldownTicks={120}
          onEngineStop={() => fg.current?.zoomToFit(400, 40)}
        />
      ) : null}
    </div>
  )
}
