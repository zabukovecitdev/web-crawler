import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { HostAnalytics } from '@/types/api'

const ALL = '__all__'

type Props = { hosts: HostAnalytics[]; value: string | undefined; onChange: (host: string | undefined) => void }

export function HostFilter({ hosts, value, onChange }: Props) {
  return (
    <Select value={value ?? ALL} onValueChange={(v) => onChange(v === ALL ? undefined : v)}>
      <SelectTrigger className="w-[200px]" aria-label="Filter status, content and timeline by host">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ALL}>All hosts</SelectItem>
        {hosts.map((h) => (
          <SelectItem key={h.host} value={h.host}>
            {h.host}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}
