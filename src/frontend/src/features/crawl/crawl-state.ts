import type { CrawlEvent, CrawlJobStatus, CrawlProgress } from '@/types/api'
import { applyCompleted, applyJobEvent, type FetchCompletedEvent } from './crawl-counters'
import { mergeRows, rowOf, type LogRow } from './crawl-rows'

export type CurrentRequest = { url: string; depth: number; sentAt: string }

export type CrawlState = {
  progress: CrawlProgress | null
  log: LogRow[]
  current: CurrentRequest | null
  seen: number[]
  lastEventId: number
}

export type CrawlAction =
  | { type: 'snapshot'; progress: CrawlProgress }
  | { type: 'history'; events: CrawlEvent[] }
  | { type: 'live'; event: CrawlEvent }

export const initialCrawlState: CrawlState = { progress: null, log: [], current: null, seen: [], lastEventId: 0 }

const FINISHED: CrawlJobStatus[] = ['Completed', 'Cancelled', 'Failed']

export function isFinished(status: CrawlJobStatus | undefined): boolean {
  return status !== undefined && FINISHED.includes(status)
}

function isCompleted(event: CrawlEvent): event is FetchCompletedEvent {
  return event.eventType === 'FetchCompleted'
}

const SEEN_LIMIT = 500

function remember(state: CrawlState, ids: number[]): Pick<CrawlState, 'seen' | 'lastEventId'> {
  return {
    seen: [...state.seen, ...ids].slice(-SEEN_LIMIT),
    lastEventId: ids.reduce((max, id) => Math.max(max, id), state.lastEventId),
  }
}

function reduceHistory(state: CrawlState, events: CrawlEvent[]): CrawlState {
  const fresh = events.filter((event) => !state.seen.includes(event.id))
  const rows = fresh.flatMap((event) => (isCompleted(event) ? [rowOf(event)] : []))
  return { ...state, log: mergeRows(state.log, rows), ...remember(state, fresh.map((event) => event.id)) }
}

function applyLive(state: CrawlState, event: CrawlEvent): CrawlState {
  if (event.eventType === 'FetchStarted') {
    return { ...state, current: { url: event.url, depth: event.depth, sentAt: event.at } }
  }
  if (isCompleted(event)) {
    const progress = state.progress ? applyCompleted(state.progress, event) : null
    return { ...state, progress, current: null, log: mergeRows(state.log, [rowOf(event)]) }
  }
  return { ...state, progress: state.progress ? applyJobEvent(state.progress, event.eventType) : null }
}

function reduceLive(state: CrawlState, event: CrawlEvent): CrawlState {
  return state.seen.includes(event.id) ? state : { ...applyLive(state, event), ...remember(state, [event.id]) }
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
