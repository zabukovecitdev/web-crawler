import { expect, it } from 'vitest'
import { eventById, HISTORY_SIZE, recentEvents, recordEmitted, streamEventId } from './crawl'

it('numbers the stream after the history, without gaps or collisions', () => {
  const ids = [0, 1, 2].flatMap((tick) => [streamEventId(tick, false), streamEventId(tick, true)])
  expect(ids).toEqual([1, 2, 3, 4, 5, 6].map((n) => HISTORY_SIZE + n))
  expect(eventById(42, ids[0]).eventType).toBe('FetchStarted')
  expect(eventById(42, ids[1]).eventType).toBe('FetchCompleted')
})

it('serves the events the stream sent, so a poll after a drop recovers them', () => {
  expect(recentEvents(42, HISTORY_SIZE)).toEqual([])
  recordEmitted(streamEventId(0, true))
  expect(recentEvents(42, HISTORY_SIZE).map((event) => event.id)).toEqual([HISTORY_SIZE + 1, HISTORY_SIZE + 2])
})
