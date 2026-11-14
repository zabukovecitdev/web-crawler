import { HubConnection, HubConnectionBuilder } from '@microsoft/signalr'
import { apiBase } from '@/lib/config'
import type { CrawlEvent } from '@/types/api'

export type ConnectionState = 'live' | 'reconnecting' | 'offline'

const RESTART_AFTER_MS = 10_000

function buildConnection(): HubConnection {
  return new HubConnectionBuilder()
    .withUrl(`${apiBase}/hubs/crawl-job`, { withCredentials: true })
    .withAutomaticReconnect()
    .build()
}

type Handlers = { onEvent: (event: CrawlEvent) => void; onState: (state: ConnectionState) => void }

function wireHandlers(connection: HubConnection, jobId: number, handlers: Handlers, onClosed: () => void): void {
  const join = () => connection.invoke('JoinJob', jobId)
  connection.on('CrawlEvent', (event: CrawlEvent) => handlers.onEvent(event))
  connection.onreconnecting(() => handlers.onState('reconnecting'))
  connection.onreconnected(() => {
    join().then(
      () => handlers.onState('live'),
      () => handlers.onState('offline'),
    )
  })
  connection.onclose(() => {
    handlers.onState('offline')
    onClosed()
  })
}

/** Connects to the crawl hub, joins the job's group and keeps trying. Returns the cleanup. */
export function connectCrawlHub(jobId: number, handlers: Handlers): () => void {
  const connection = buildConnection()
  let stopped = false
  let timer: ReturnType<typeof setTimeout> | undefined

  const start = () => {
    connection
      .start()
      .then(() => connection.invoke('JoinJob', jobId))
      .then(() => handlers.onState('live'))
      .catch(() => scheduleRestart())
  }
  const scheduleRestart = () => {
    handlers.onState('offline')
    if (!stopped) {
      timer = setTimeout(start, RESTART_AFTER_MS)
    }
  }

  wireHandlers(connection, jobId, handlers, scheduleRestart)
  start()

  return () => {
    stopped = true
    clearTimeout(timer)
    connection
      .invoke('LeaveJob', jobId)
      .catch(() => undefined)
      .finally(() => void connection.stop())
  }
}
