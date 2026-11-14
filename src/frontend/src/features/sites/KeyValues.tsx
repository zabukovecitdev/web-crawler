import type { ReactNode } from 'react'

export type KeyValueRow = [label: string, value: ReactNode]

/** Label and mono value pairs, as the design's `kv` list. */
export function KeyValues({ rows }: { rows: KeyValueRow[] }) {
  return (
    <dl className="m-0 grid grid-cols-[max-content_minmax(0,1fr)] items-baseline gap-x-5 gap-y-2.5">
      {rows.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="label">{label}</dt>
          <dd className="m-0 font-mono text-[13px] font-medium leading-[18px] [overflow-wrap:anywhere]">{value}</dd>
        </div>
      ))}
    </dl>
  )
}
