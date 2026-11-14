import type { Tone } from '@/components/shared/status'
import type { ContentState, SiteStatus } from '@/types/api'

export const CONTENT_LABELS: Record<ContentState, string> = {
  ParsedIndexed: 'Parsed, indexed',
  ParsedWaitingForIndex: 'Parsed, waiting for index',
  WaitingForParse: 'Waiting for parse',
  Redirect: 'Redirect, no content',
  Skipped: 'Skipped, not HTML',
  NotStored: 'Not stored',
  Failed: 'Failed',
}

export const SITE_STATUS: Record<SiteStatus, { word: string; tone: Tone; live: boolean }> = {
  Crawling: { word: 'Crawling', tone: 'success', live: true },
  Paused: { word: 'Paused', tone: 'warning', live: true },
  Crawled: { word: 'Crawled', tone: 'neutral', live: false },
  Never: { word: 'Never crawled', tone: 'neutral', live: false },
}
