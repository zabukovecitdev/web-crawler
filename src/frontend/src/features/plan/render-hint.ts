import type { JsVerdict, RenderMode } from '@/types/api'
import { usesBrowser } from './plan-summary'

export type HintTone = 'muted' | 'error' | 'warn'

export type RenderHint = { text: string; tone: HintTone }

export type Blocker = 'always' | 'site' | null

export function renderHint(mode: RenderMode, verdict: JsVerdict, browserUp: boolean): RenderHint {
  const needs = verdict !== 'NotNeeded'
  if (mode === 'Auto') {
    return autoHint(needs, browserUp)
  }
  if (mode === 'Always') {
    return browserUp
      ? { text: 'Every page is rendered in a browser. About 3 s per page.', tone: 'muted' }
      : { text: 'Every page is rendered in a browser, but the browser service is not available.', tone: 'error' }
  }
  return needs
    ? { text: 'Pages are fetched over HTTP only. This site needs JavaScript, so pages will be stored almost empty.', tone: 'warn' }
    : { text: 'Pages are fetched over HTTP only.', tone: 'muted' }
}

function autoHint(needs: boolean, browserUp: boolean): RenderHint {
  if (!needs) {
    return { text: 'Detected: not needed. Pages are fetched over HTTP. A page that turns out to need JavaScript is rendered on its own.', tone: 'muted' }
  }
  return browserUp
    ? { text: 'Detected: this site needs JavaScript. Every page is rendered in a browser, about 3 s per page.', tone: 'muted' }
    : { text: 'Detected: this site needs JavaScript, but the browser service is not available.', tone: 'error' }
}

export function blockerOf(mode: RenderMode, verdict: JsVerdict, browserUp: boolean): Blocker {
  if (browserUp || !usesBrowser(mode, verdict)) {
    return null
  }
  return mode === 'Always' ? 'always' : 'site'
}
