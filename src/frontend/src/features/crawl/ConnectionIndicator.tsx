import { Led } from '@/components/shared/Led'
import type { ConnectionState } from './crawl-hub'

const WORDS: Record<ConnectionState, string> = {
  live: 'Live',
  reconnecting: 'Reconnecting',
  offline: 'Offline, checking every 3 s',
}

/** Live / Reconnecting / Offline. The LED is lit only while live. */
export function ConnectionIndicator({ state }: { state: ConnectionState }) {
  return (
    <p role="status" className="label m-0 inline-flex items-center gap-2">
      {state === 'live' ? <Led /> : <span aria-hidden="true" className="inline-block size-2 rounded-full bg-ink-faint" />}
      {WORDS[state]}
    </p>
  )
}
