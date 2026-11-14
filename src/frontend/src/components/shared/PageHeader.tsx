import type { ReactNode } from 'react'

type Props = { kicker: string; title: string; actions?: ReactNode }

/** Label kicker over the page's h1, actions aligned to the right (wrapping on phones). */
export function PageHeader({ kicker, title, actions }: Props) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div className="flex min-w-0 flex-col gap-2">
        <p className="label">{kicker}</p>
        <h1 className="m-0 break-words text-[32px] font-semibold leading-[38px] tracking-[-0.02em]">{title}</h1>
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-3">{actions}</div> : null}
    </div>
  )
}
