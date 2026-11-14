import { Badge } from '@/components/ui/badge'
import { Led } from './Led'
import { httpStatusLook, type Tone } from './status'

/** HTTP status: "200 OK", "404 Client error". Word and mark, not just colour. */
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

/** A badge with a word and, for live states, an LED. */
export function ToneBadge({ tone, live = false, children }: { tone: Tone; live?: boolean; children: string }) {
  return (
    <Badge variant={tone}>
      {live ? <Led /> : null}
      {children}
    </Badge>
  )
}
