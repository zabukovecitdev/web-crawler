import { useMemo } from 'react'
import { formatNumber } from '@/lib/format'
import type { HostLinkEdge } from '@/types/api'
import { AnalyticsCard, PanelState } from './AnalyticsCard'
import { EdgeList } from './EdgeList'
import { buildHostGraph } from './host-graph'
import { HostGraphCanvas } from './HostGraphCanvas'

type Query = Parameters<typeof PanelState>[0]['query'] & { data?: HostLinkEdge[] }
type Props = { query: Query; focus: string | undefined; onFocus: (host: string | undefined) => void }

const LIST_LIMIT = 10

function Empty() {
  return (
    <p className="m-0 py-6 text-[13px] text-muted-foreground">
      No links between different sites in this range. Pages parsed before link tracking need a reprocess.
    </p>
  )
}

export function HostGraphCard({ query, focus, onFocus }: Props) {
  const edges = query.data
  const graph = useMemo(() => buildHostGraph(edges ?? []), [edges])
  const kicker = focus
    ? `Links to and from ${focus} · click the background to show every site`
    : `${formatNumber(graph.nodes.length)} sites · ${formatNumber(graph.links.length)} connections · arrows point to the linked site`
  return (
    <AnalyticsCard kicker={kicker} title="Which sites link to which">
      <PanelState query={query}>
        {graph.nodes.length === 0 ? (
          <Empty />
        ) : (
          <>
            <HostGraphCanvas graph={graph} focus={focus} onFocus={onFocus} />
            <p className="m-0 text-xs text-muted-foreground">
              Circle size: links in plus out. Line width: number of links. Drag a site to move it, scroll to zoom, click a site to filter.
            </p>
            <EdgeList edges={graph.links.slice(0, LIST_LIMIT)} />
          </>
        )}
      </PanelState>
    </AnalyticsCard>
  )
}
