import { Link } from 'react-router-dom'
import { formatNumber } from '@/lib/format'
import type { GraphLink } from './host-graph'

/** The same connections as text: readable, searchable and usable without a pointer. */
export function EdgeList({ edges }: { edges: GraphLink[] }) {
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
