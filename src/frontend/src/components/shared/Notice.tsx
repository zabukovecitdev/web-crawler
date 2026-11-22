import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

export type NoticeTone = 'error' | 'warn' | 'info'

type Props = { tone: NoticeTone; title: string; children?: ReactNode; actions?: ReactNode }

const LOOK: Record<NoticeTone, { box: string; mark: string; icon: string; role: 'alert' | 'status' }> = {
  error: { box: 'border-signal-300 bg-signal-100', mark: 'bg-signal-ink text-card', icon: '×', role: 'alert' },
  warn: { box: 'border-amber-ink/40 bg-amber-soft', mark: 'bg-amber-ink text-card', icon: '!', role: 'status' },
  info: { box: 'border-border bg-card', mark: 'bg-muted-foreground text-card', icon: 'i', role: 'status' },
}

export function Notice({ tone, title, children, actions }: Props) {
  const look = LOOK[tone]
  return (
    <div role={look.role} className={cn('flex gap-3 rounded-lg border p-4', look.box)}>
      <span aria-hidden="true" className={cn('mt-0.5 flex size-5 shrink-0 items-center justify-center rounded-full text-xs font-semibold', look.mark)}>
        {look.icon}
      </span>
      <div className="flex min-w-0 flex-col gap-1.5 text-[14px] leading-5">
        <p className="m-0 font-semibold">{title}</p>
        {children}
        {actions ? <div className="mt-1 flex flex-wrap items-center gap-2">{actions}</div> : null}
      </div>
    </div>
  )
}
