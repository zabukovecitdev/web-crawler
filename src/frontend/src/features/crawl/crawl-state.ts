import type { CrawlEvent, CrawlJobStatus, CrawlProgress } from '@/types/api'
import type { StoredEvent } from './crawl-api'
import { applyCompleted, applyJobEvent, type FetchCompletedEvent } from './crawl-counters'
import { mergeRows, rowOf, type LogRow } from './crawl-rows'

export type CurrentRequest = { url: string; depth: number; sentAt: string }

export type CrawlState = {
  progress: CrawlProgress | null
  log: LogRow[]
  current: CurrentRequest | null
  lastEventId: number
}

export type CrawlAction =
  | { type: 'snapshot'; progress: CrawlProgress }
  | { type: 'history'; events: StoredEvent[] }
  | { type: 'live'; event: CrawlEvent }

export const initialCrawlState: CrawlState = { progress: null, log: [], current: null, lastEventId: 0 }

const FINISHED: CrawlJobStatus[] = ['Completed', 'Cancelled', 'Failed']

export function isFinished(status: CrawlJobStatus | undefined): boolean {
  return status !== undefined && FINISHED.includes(status)
}

function isCompleted(event: CrawlEvent): event is FetchCompletedEvent {
  return event.eventType === 'FetchCompleted'
}

function reduceHistory(state: CrawlState, events: StoredEvent[]): CrawlState {
  const fresh = events.filter((event) => event.id > state.lastEventId)
  const rows = fresh.flatMap((event) => (isCompleted(event) ? [rowOf(event)] : []))
  const lastEventId = fresh.reduce((max, event) => Math.max(max, event.id), state.lastEventId)
  return { ...state, log: mergeRows(state.log, rows), lastEventId }
}

function reduceLive(state: CrawlState, event: CrawlEvent): CrawlState {
  if (event.eventType === 'FetchStarted') {
    return { ...state, current: { url: event.url, depth: event.depth, sentAt: event.at } }
  }
  if (isCompleted(event)) {
    const progress = state.progress ? applyCompleted(state.progress, event) : null
    return { ...state, progress, current: null, log: mergeRows(state.log, [rowOf(event)]) }
  }
  return { ...state, progress: state.progress ? applyJobEvent(state.progress, event.eventType) : null }
}

export function crawlReducer(state: CrawlState, action: CrawlAction): CrawlState {
  switch (action.type) {
    case 'snapshot':
      return { ...state, progress: action.progress }
    case 'history':
      return reduceHistory(state, action.events)
    case 'live':
      return reduceLive(state, action.event)
  }
}
