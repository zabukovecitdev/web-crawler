import type { JsVerdict, RenderMode } from '@/types/api'
import { usesBrowser } from './plan-summary'
import { blockerOf } from './render-hint'

export type NoticeKinds = { gate: boolean; never: boolean; browserInfo: boolean; sitemapSlow: boolean }

export type NoticeInput = { mode: RenderMode; verdict: JsVerdict; browserUp: boolean; sitemapWaitedTooLong: boolean }

export function noticeKinds(input: NoticeInput): NoticeKinds {
  const { mode, verdict, browserUp } = input
  return {
    gate: blockerOf(mode, verdict, browserUp) !== null,
    never: mode === 'Never' && verdict !== 'NotNeeded',
    browserInfo: !browserUp && mode === 'Auto' && !usesBrowser(mode, verdict),
    sitemapSlow: input.sitemapWaitedTooLong,
  }
}
