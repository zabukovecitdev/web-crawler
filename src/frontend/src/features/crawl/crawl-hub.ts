import { HttpError, HubConnectionBuilder, HubConnectionState, type HubConnection } from '@microsoft/signalr'
import { apiBase } from '@/lib/config'
import type { LiveEvent } from './crawl-api'

export type ConnectionState = 'connecting' | 'live' | 'reconnecting' | 'offline'

export type HubHandlers = {
  onEvent: (event: LiveEvent) => void
  onState: (state: ConnectionState) => void
  onUnauthorized: () => void
}

const RETRY_MS = 10_000

type Session = {
  connection: HubConnection
  jobId: number
  handlers: HubHandlers
  stopped: boolean
  timer: ReturnType<typeof setTimeout> | undefined
}

function buildConnection(): HubConnection {
  // The CSRF middleware wants this header on every cookie POST, the negotiate request included.
  return new HubConnectionBuilder()
    .withUrl(`${apiBase}/hubs/crawl-job`, { withCredentials: true, headers: { 'X-Requested-With': 'samobot' } })
    .withAutomaticReconnect()
    .build()
}

/** True for a 401 from the hub; negotiate failures wrap the status code in the message only. */
export function isUnauthorized(error: unknown): boolean {
  if (error instanceof HttpError && error.statusCode === 401) {
    return true
  }
  return /Status code '401'/.test(String(error))
}

function emit(s: Session, state: ConnectionState): void {
  if (!s.stopped) {
    s.handlers.onState(state)
  }
}

function retryLater(s: Session, fn: (s: Session) => void): void {
  if (!s.stopped) {
    clearTimeout(s.timer)
    s.timer = setTimeout(() => fn(s), RETRY_MS)
  }
}

function fail(s: Session, error: unknown, retry: (s: Session) => void): void {
  if (s.stopped) {
    return
  }
  emit(s, 'offline')
  if (isUnauthorized(error)) {
    s.stopped = true
    clearTimeout(s.timer)
    s.handlers.onUnauthorized()
    return
  }
  retryLater(s, retry)
}

function join(s: Session): void {
  s.connection.invoke('JoinJob', s.jobId).then(
    () => emit(s, 'live'),
    (error: unknown) => fail(s, error, join),
  )
}

/** Never calls start() on a connected hub: a connected one only re-joins. */
function start(s: Session): void {
  if (s.stopped) {
    return
  }
  const { state } = s.connection
  if (state === HubConnectionState.Disconnected) {
    s.connection.start().then(
      () => join(s),
      (error: unknown) => fail(s, error, start),
    )
  } else if (state === HubConnectionState.Connected) {
    join(s)
  } else {
    retryLater(s, start)
  }
}

function wire(s: Session): void {
  s.connection.on('CrawlEvent', (event: LiveEvent) => s.handlers.onEvent(event))
  s.connection.onreconnecting(() => emit(s, 'reconnecting'))
  s.connection.onreconnected(() => join(s))
  s.connection.onclose(() => {
    emit(s, 'offline')
    retryLater(s, start)
  })
}

function stop(s: Session): void {
  s.stopped = true
  clearTimeout(s.timer)
  if (s.connection.state === HubConnectionState.Connected) {
    s.connection
      .invoke('LeaveJob', s.jobId)
      .catch(() => undefined)
      .finally(() => void s.connection.stop())
  } else {
    void s.connection.stop()
  }
}

/** Connects to the crawl hub, joins the job's group and keeps trying. Returns the cleanup. */
export function connectCrawlHub(jobId: number, handlers: HubHandlers): () => void {
  const s: Session = { connection: buildConnection(), jobId, handlers, stopped: false, timer: undefined }
  wire(s)
  emit(s, 'connecting')
  start(s)
  return () => stop(s)
}
