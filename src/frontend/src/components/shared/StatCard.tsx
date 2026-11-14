import type { ReactNode } from 'react'
import { Card } from '@/components/ui/card'

type Props = { kicker: string; value: ReactNode; note?: ReactNode }

/** Kicker label + large mono readout + optional one-line note. */
export function StatCard({ kicker, value, note }: Props) {
  return (
    <Card className="flex flex-col gap-1 p-5 shadow-zb-1">
      <p className="label">{kicker}</p>
      <div className="readout-lg">{value}</div>
      {note ? <p className="text-[13px] leading-[18px] text-muted-foreground">{note}</p> : null}
    </Card>
  )
}
