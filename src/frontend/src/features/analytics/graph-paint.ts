import type { NodeObject } from 'react-force-graph-2d'
import type { GraphPalette } from './graph-palette'
import type { GraphNode } from './host-graph'

export type Node = NodeObject<GraphNode>

export type PaintOptions = {
  radius: number
  palette: GraphPalette
  dim: boolean
  selected: boolean
  labelled: boolean
}

function drawCircle(node: Node, ctx: CanvasRenderingContext2D, zoom: number, o: PaintOptions): void {
  ctx.beginPath()
  ctx.arc(node.x ?? 0, node.y ?? 0, o.radius, 0, 2 * Math.PI)
  ctx.fillStyle = o.selected ? o.palette.signal : o.palette.node
  ctx.fill()
  ctx.lineWidth = 1 / zoom
  ctx.strokeStyle = o.palette.ink
  ctx.stroke()
}

/** The host name above the circle, with a halo so lines behind it do not cut through the letters. */
function drawLabel(node: Node, ctx: CanvasRenderingContext2D, zoom: number, o: PaintOptions): void {
  const x = node.x ?? 0
  const y = (node.y ?? 0) - o.radius - 2 / zoom
  ctx.font = `${11 / zoom}px Geist, system-ui, sans-serif`
  ctx.textAlign = 'center'
  ctx.textBaseline = 'bottom'
  ctx.lineWidth = 3 / zoom
  ctx.strokeStyle = o.palette.halo
  ctx.strokeText(String(node.id), x, y)
  ctx.fillStyle = o.palette.ink
  ctx.fillText(String(node.id), x, y)
}

export function paintNode(node: Node, ctx: CanvasRenderingContext2D, zoom: number, o: PaintOptions): void {
  ctx.globalAlpha = o.dim ? 0.2 : 1
  drawCircle(node, ctx, zoom, o)
  if (o.labelled) {
    drawLabel(node, ctx, zoom, o)
  }
  ctx.globalAlpha = 1
}

/** The area where the pointer counts as being on the node: the circle plus a little slack. */
export function paintHitArea(node: Node, colour: string, ctx: CanvasRenderingContext2D, radius: number): void {
  ctx.fillStyle = colour
  ctx.beginPath()
  ctx.arc(node.x ?? 0, node.y ?? 0, radius + 2, 0, 2 * Math.PI)
  ctx.fill()
}
