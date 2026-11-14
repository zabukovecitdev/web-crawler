import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { connectCrawlHub, type ConnectionState } from './crawl-hub'

type Fake = {
  state: string
  start: ReturnType<typeof vi.fn>
  stop: ReturnType<typeof vi.fn>
  invoke: ReturnType<typeof vi.fn>
  on: ReturnType<typeof vi.fn>
  handlers: Record<string, () => void>
}

const fake = vi.hoisted(() => ({ current: null as unknown }))

vi.mock('@microsoft/signalr', () => {
  const builder = {
    withUrl: () => builder,
    withAutomaticReconnect: () => builder,
    build: () => fake.current,
  }
  return {
    HttpError: class extends Error {},
    HubConnectionBuilder: function () {
      return builder
    },
    HubConnectionState: { Disconnected: 'Disconnected', Connected: 'Connected', Reconnecting: 'Reconnecting' },
  }
})

function makeFake(): Fake {
  const handlers: Record<string, () => void> = {}
  const f: Fake = {
    state: 'Disconnected',
    handlers,
    start: vi.fn(() => {
      f.state = 'Connected'
      return Promise.resolve()
    }),
    stop: vi.fn(() => Promise.resolve()),
    invoke: vi.fn(() => Promise.resolve()),
    on: vi.fn(),
  }
  Object.assign(f, {
    onreconnecting: (fn: () => void) => (handlers.reconnecting = fn),
    onreconnected: (fn: () => void) => (handlers.reconnected = fn),
    onclose: (fn: () => void) => (handlers.close = fn),
  })
  return f
}

let f: Fake
const states: ConnectionState[] = []
const onUnauthorized = vi.fn()

function connect() {
  return connectCrawlHub(7, { onEvent: vi.fn(), onState: (s) => states.push(s), onUnauthorized })
}

beforeEach(() => {
  vi.useFakeTimers()
  f = makeFake()
  fake.current = f
  states.length = 0
  onUnauthorized.mockClear()
})
afterEach(() => vi.useRealTimers())

it('goes connecting then live and joins the job', async () => {
  connect()
  await vi.advanceTimersByTimeAsync(0)
  expect(states).toEqual(['connecting', 'live'])
  expect(f.invoke).toHaveBeenCalledWith('JoinJob', 7)
})

it('re-joins after a reconnect without calling start again', async () => {
  connect()
  await vi.advanceTimersByTimeAsync(0)
  f.handlers.reconnecting()
  f.handlers.reconnected()
  await vi.advanceTimersByTimeAsync(0)
  expect(states.slice(-2)).toEqual(['reconnecting', 'live'])
  expect(f.start).toHaveBeenCalledTimes(1)
})

it('retries the start 10 s after a close', async () => {
  connect()
  await vi.advanceTimersByTimeAsync(0)
  f.state = 'Disconnected'
  f.handlers.close()
  await vi.advanceTimersByTimeAsync(10_000)
  expect(f.start).toHaveBeenCalledTimes(2)
})

it('retries only the join when the join fails', async () => {
  f.invoke.mockRejectedValueOnce(new Error('boom'))
  connect()
  await vi.advanceTimersByTimeAsync(10_000)
  expect(states).toEqual(['connecting', 'offline', 'live'])
  expect(f.start).toHaveBeenCalledTimes(1)
})

it('stops for good and signs out on a 401', async () => {
  f.start.mockRejectedValueOnce(new Error("Status code '401'"))
  connect()
  await vi.advanceTimersByTimeAsync(30_000)
  expect(onUnauthorized).toHaveBeenCalledTimes(1)
  expect(f.start).toHaveBeenCalledTimes(1)
})

it('is silent after cleanup, even when a pending start fails', async () => {
  let reject: (e: Error) => void = () => undefined
  f.start.mockReturnValueOnce(new Promise<void>((_, r) => (reject = r)))
  const stop = connect()
  stop()
  reject(new Error('late'))
  await vi.advanceTimersByTimeAsync(30_000)
  expect(states).toEqual(['connecting'])
  expect(f.start).toHaveBeenCalledTimes(1)
})
