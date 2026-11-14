import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import type { AnalyticsRange } from '@/types/api'
import { parseRange, RANGES } from './range'

type Props = { value: AnalyticsRange; onChange: (range: AnalyticsRange) => void }

export function RangeTabs({ value, onChange }: Props) {
  return (
    <Tabs value={value} onValueChange={(v) => onChange(parseRange(v))}>
      <TabsList aria-label="Time range">
        {RANGES.map((r) => (
          <TabsTrigger key={r.value} value={r.value}>
            {r.label}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  )
}
