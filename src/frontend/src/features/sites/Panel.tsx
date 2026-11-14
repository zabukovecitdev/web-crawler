import type { ReactNode } from 'react'
import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'

type Props = { title: string; kicker?: string; children: ReactNode; className?: string }

/** A card with an optional mono kicker over a heading. */
export function Panel({ title, kicker, children, className }: Props) {
  return (
    <Card className={cn('flex min-w-0 flex-col gap-5 p-6 shadow-zb-1', className)}>
      <div className="flex flex-col gap-1.5">
        {kicker ? <p className="label">{kicker}</p> : null}
        <h2 className="m-0 text-lg font-semibold leading-6">{title}</h2>
      </div>
      {children}
    </Card>
  )
}
