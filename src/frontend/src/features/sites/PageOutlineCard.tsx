import type { ParsedPageDto } from '@/types/api'
import { Panel } from './Panel'

/** The headings of the page as an indented outline: the structure of the content at a glance. */
export function PageOutlineCard({ parsed }: { parsed: ParsedPageDto | null }) {
  const headings = parsed?.headings ?? []
  return (
    <Panel title="Outline" kicker={`${headings.length} heading${headings.length === 1 ? '' : 's'}`}>
      {headings.length === 0 ? (
        <p className="m-0 text-[13px] text-muted-foreground">
          {parsed ? 'This page has no headings.' : 'No outline: the page was not parsed.'}
        </p>
      ) : (
        <ul className="m-0 flex max-h-[360px] list-none flex-col gap-1.5 overflow-y-auto p-0 text-[13px] leading-[18px]">
          {headings.map((h, i) => (
            <li key={i} style={{ paddingLeft: `${(h.level - 1) * 12}px` }} className="flex items-baseline gap-2">
              <span className="label w-6 shrink-0">H{h.level}</span>
              <span className="min-w-0 [overflow-wrap:anywhere]">{h.text}</span>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  )
}
