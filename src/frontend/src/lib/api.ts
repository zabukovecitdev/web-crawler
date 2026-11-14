import { apiBase } from './config'

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

let onUnauthorized: (() => void) | null = null

/** AuthProvider registers what happens when any request comes back 401. */
export function setUnauthorizedHandler(handler: (() => void) | null): void {
  onUnauthorized = handler
}

function isWrite(init?: RequestInit): boolean {
  const method = (init?.method ?? 'GET').toUpperCase()
  return method !== 'GET' && method !== 'HEAD'
}

function buildHeaders(init?: RequestInit): Headers {
  const headers = new Headers(init?.headers)
  headers.set('Accept', 'application/json')
  if (isWrite(init)) {
    headers.set('X-Requested-With', 'samobot')
  }
  if (init?.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }
  return headers
}

async function toError(res: Response): Promise<ApiError> {
  const text = await res.text()
  return new ApiError(res.status, text || `${res.status} ${res.statusText}`)
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${apiBase}${path}`, {
    ...init,
    credentials: 'include',
    headers: buildHeaders(init),
  })
  if (res.status === 401) {
    onUnauthorized?.()
  }
  if (!res.ok) {
    throw await toError(res)
  }
  if (res.status === 204) {
    return undefined as T
  }
  return res.json() as Promise<T>
}

type QueryValue = string | number | undefined | null

/** Builds `?a=1&b=2`, skipping empty values. */
export function queryString(params: Record<string, QueryValue>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, String(value))
    }
  }
  const text = search.toString()
  return text ? `?${text}` : ''
}
