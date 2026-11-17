import { Link } from 'react-router-dom'
import { formatNumber } from '@/lib/format'
import type { GraphEdge, GraphNode } from './host-graph'

const INK = 'rgb(var(--foreground))'
const FAINT = 'rgb(var(--muted-foreground))'
const NODE = 'rgb(var(--chart-1))'
const SIGNAL = 'rgb(var(--primary))'

export const ARROW_ID = 'host-graph-arrow'

export function ArrowMarker() {
  return (
    <defs>
      <marker id={ARROW_ID} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
        <path d="M0 0 L10 5 L0 10 z" fill={FAINT} />
      </marker>
    </defs>
  )
}

/** A line from the edge of one circle to the edge of the other, with the arrow at the linked site. */
export function EdgeLine({ edge, dim }: { edge: GraphEdge; dim: boolean }) {
  const dx = edge.target.x - edge.source.x
  const dy = edge.target.y - edge.source.y
  const len = Math.hypot(dx, dy) || 1
  const gap = edge.target.radius + 3
  return (
    <line
      x1={edge.source.x + (dx / len) * edge.source.radius}
      y1={edge.source.y + (dy / len) * edge.source.radius}
      x2={edge.target.x - (dx / len) * gap}
      y2={edge.target.y - (dy / len) * gap}
      stroke={FAINT}
      strokeWidth={edge.width}
      strokeOpacity={dim ? 0.12 : 0.55}
      markerEnd={`url(#${ARROW_ID})`}
    >
      <title>{`${edge.fromHost} → ${edge.toHost}: ${formatNumber(edge.links)} links on ${formatNumber(edge.pages)} pages`}</title>
    </line>
  )
}

type NodeProps = {
  node: GraphNode
  dim: boolean
  selected: boolean
  labelled: boolean
  onToggle: () => void
  onHover: (host: string | undefined) => void
}

/** A site as a circle. It is a button: click or Enter filters the analytics to it. */
export function NodeMark({ node, dim, selected, labelled, onToggle, onHover }: NodeProps) {
  const summary = `${node.host}: ${formatNumber(node.out)} links out, ${formatNumber(node.in)} in`
  return (
    <g
      transform={`translate(${node.x} ${node.y})`}
      opacity={dim ? 0.25 : 1}
      role="button"
      tabIndex={0}
      aria-pressed={selected}
      aria-label={summary}
      className="cursor-pointer outline-none [&:focus-visible>circle]:stroke-[3px]"
      onClick={onToggle}
      onKeyDown={(ev) => {
        if (ev.key === 'Enter' || ev.key === ' ') {
          ev.preventDefault()
          onToggle()
        }
      }}
      onMouseEnter={() => onHover(node.host)}
      onMouseLeave={() => onHover(undefined)}
      onFocus={() => onHover(node.host)}
      onBlur={() => onHover(undefined)}
    >
      <circle r={node.radius} fill={selected ? SIGNAL : NODE} stroke={INK} strokeWidth={selected ? 2 : 1} />
      {labelled ? (
        <text y={-node.radius - 5} textAnchor="middle" fontSize={11} fill={INK} className="pointer-events-none select-none">
          {node.host}
        </text>
      ) : null}
      <title>{summary}</title>
    </g>
  )
}

/** The same connections as text: readable, searchable and usable without a pointer. */
export function EdgeList({ edges }: { edges: GraphEdge[] }) {
  return (
    <ol className="m-0 flex list-none flex-col p-0 text-[13px] leading-[18px]">
      {edges.map((e) => (
        <li key={`${e.fromHost}>${e.toHost}`} className="flex items-center gap-2 border-t border-border py-[7px] first:border-t-0">
          <Link className="truncate text-foreground underline-offset-2 hover:underline" to={`/sites/${e.fromHost}`}>
            {e.fromHost}
          </Link>
          <span aria-label="links to">→</span>
          <Link className="truncate text-foreground underline-offset-2 hover:underline" to={`/sites/${e.toHost}`}>
            {e.toHost}
          </Link>
          <span className="ml-auto whitespace-nowrap text-muted-foreground">
            {formatNumber(e.links)} links · {formatNumber(e.pages)} pages
          </span>
        </li>
      ))}
    </ol>
  )
}
