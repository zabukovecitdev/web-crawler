import { useMemo, useState } from 'react'
import { formatNumber } from '@/lib/format'
import type { HostLinkEdge } from '@/types/api'
import { AnalyticsCard, PanelState } from './AnalyticsCard'
import { buildHostGraph, type GraphNode, type HostGraph, neighbours } from './host-graph'
import { ArrowMarker, EdgeList, EdgeLine, NodeMark } from './HostGraphParts'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: HostLinkEdge[] }
type Props = { query: Query; focus: string | undefined; onFocus: (host: string | undefined) => void }

const LABEL_LIMIT = 20
const LIST_LIMIT = 10

/** A label for every site in a small graph, and only for the busiest ones in a crowded one. */
function labelled(nodes: GraphNode[]): Set<string> {
  const ranked = [...nodes].sort((a, b) => b.in + b.out - (a.in + a.out))
  return new Set(ranked.slice(0, LABEL_LIMIT).map((n) => n.host))
}

function Empty() {
  return (
    <p className="m-0 py-6 text-[13px] text-muted-foreground">
      No links between different sites in this range. Pages parsed before link tracking need a reprocess.
    </p>
  )
}

type DrawingProps = { graph: HostGraph; focus: string | undefined; onFocus: Props['onFocus'] }

function Drawing({ graph, focus, onFocus }: DrawingProps) {
  const labels = useMemo(() => labelled(graph.nodes), [graph])
  const [hover, setHover] = useState<string | undefined>()
  const active = hover ?? focus
  const near = active ? neighbours(graph, active) : undefined
  return (
    <svg
      viewBox={`0 0 ${graph.width} ${graph.height}`}
      className="h-auto w-full rounded-md bg-muted/40"
      role="group"
      aria-label="Graph of links between sites. The same data is listed below."
    >
      <ArrowMarker />
      {graph.edges.map((e) => (
        <EdgeLine key={`${e.fromHost}>${e.toHost}`} edge={e} dim={!!active && e.fromHost !== active && e.toHost !== active} />
      ))}
      {graph.nodes.map((n) => (
        <NodeMark
          key={n.host}
          node={n}
          dim={!!near && !near.has(n.host)}
          selected={n.host === focus}
          labelled={labels.has(n.host) || n.host === active}
          onToggle={() => onFocus(n.host === focus ? undefined : n.host)}
          onHover={setHover}
        />
      ))}
    </svg>
  )
}

export function HostGraphCard({ query, focus, onFocus }: Props) {
  const edges = query.data
  const graph = useMemo(() => buildHostGraph(edges ?? []), [edges])
  const kicker = focus
    ? `Links to and from ${focus} · click it again to show every site`
    : `${formatNumber(graph.nodes.length)} sites · ${formatNumber(graph.edges.length)} connections · arrows point to the linked site`
  return (
    <AnalyticsCard kicker={kicker} title="Which sites link to which">
      <PanelState query={query}>
        {graph.nodes.length === 0 ? (
          <Empty />
        ) : (
          <>
            <Drawing graph={graph} focus={focus} onFocus={onFocus} />
            <p className="m-0 text-xs text-muted-foreground">
              Circle size: links in plus out. Line width: number of links. Hover a site to see its neighbours, click to filter.
            </p>
            <EdgeList edges={graph.edges.slice(0, LIST_LIMIT)} />
          </>
        )}
      </PanelState>
    </AnalyticsCard>
  )
}
