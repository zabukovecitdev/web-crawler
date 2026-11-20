import { Badge } from '@/components/ui/badge'
import { Led } from './Led'
import { httpStatusLook, type Tone } from './status'

export function StatusBadge({ code }: { code: number | null | undefined }) {
  const look = httpStatusLook(code)
  return (
    <Badge variant={look.tone}>
      <span aria-hidden="true">{look.mark}</span>
      <span className="readout">{code ?? '—'}</span>
      {look.word}
    </Badge>
  )
}

export function ToneBadge({ tone, live = false, children }: { tone: Tone; live?: boolean; children: string }) {
  return (
    <Badge variant={tone}>
      {live ? <Led /> : null}
      {children}
    </Badge>
  )
}
